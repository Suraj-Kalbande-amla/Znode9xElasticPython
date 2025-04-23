from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import parallel_bulk
from elasticsearch.exceptions import BadRequestError, ConnectionError, TransportError, ApiError, NotFoundError
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import json

logs =[]


def convert_json(value):
    try:
        return json.loads(value) 
    except (json.JSONDecodeError, TypeError):
        return value 
def chunkify(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]
def clean_value(val):
    if isinstance(val, float):
        if np.isnan(val) or np.isinf(val):  
            return None  
    elif isinstance(val, str):
        try:
            return json.loads(val) 
        except json.JSONDecodeError:
            return val 
    return val

def policy_exists(policy_name,es):
    try:
        es.ilm.get_lifecycle(name=policy_name)
        return True
    except NotFoundError:
        return False    
def create_policy(es):
    policy_delete = "delete-after-1day"
    policy_merge = "Merge-segment-800"
    policy_body_delet = {
        "policy": {
            "phases": {
                "hot": {
                    "actions": {}
                },
                "delete": {
                    "min_age": "1d",
                    "actions": {
                        "delete": {}
                        }
                    }
                }
            }
        }
    policy_body_merge = {  "policy": {
						"phases": {
						  "warm": {
                            "min_age": "10m",
							"actions": {
							  "forcemerge": {
								"max_num_segments": 1
							  },
							  "set_priority": {
								"priority": 800
							  }
							}
						  }
						}
					  }}
    if not policy_exists(policy_delete,es):
        es.ilm.put_lifecycle(name=policy_delete, body=policy_body_delet)
    else:
        current = es.ilm.get_lifecycle(name=policy_delete)[policy_delete]["policy"]
        if  current !=  policy_body_delet["policy"] :
            es.ilm.put_lifecycle(name=policy_delete, body=policy_body_delet) 
    if not policy_exists(policy_merge,es):
      es.ilm.put_lifecycle(name=policy_merge, body=policy_body_merge) 
    else:
        current = es.ilm.get_lifecycle(name=policy_merge)[policy_merge]["policy"]
        if  current !=  policy_body_merge["policy"] :
            es.ilm.put_lifecycle(name=policy_merge, body=policy_body_merge)   
    return True
def calculate_avg_record_size(actions):
    total_size = 0
    total_records = len(actions)

    for action in actions:
        doc = action.get("_source") or action.get("doc")
        if doc is None:
            continue  # Skip invalid actions (optional)
        try:
            source_json = json.dumps(doc)
            total_size += len(source_json.encode('utf-8'))
        except Exception as e:
            logs.append({
                "status": "error",
                "type": type(e).__name__,
                "message": str(e)
            })
            continue

    avg_size = total_size / total_records if total_records else 0
    return avg_size        
def get_dynamic_chunk_size(es: Elasticsearch, avg_record_size_bytes=7000, max_chunk=10000, base_chunk=5000, min_chunk=500):
    try:
        nodes_stats = es.nodes.stats(metric=["jvm", "thread_pool", "os"])
        total_heap = used_heap = 0
        queue_capacity, active_bulk_threads = [], []
        total_cpu_cores = 0
        load_averages = []

        for node in nodes_stats['nodes'].values():
            # Heap stats
            heap = node['jvm']['mem']
            total_heap += heap.get('heap_max_in_bytes', 0)
            used_heap += heap.get('heap_used_in_bytes', 0)

            # Thread pool stats
            bulk_pool = node.get('thread_pool', {}).get('bulk', {})
            queue_capacity.append(bulk_pool.get('queue', 0))
            active_bulk_threads.append(bulk_pool.get('active', 0))

            # OS (CPU) stats
            os_info = node.get('os', {})
            cpu_cores = os_info.get('available_processors', 1)
            total_cpu_cores += cpu_cores

            cpu_load_avg = os_info.get('cpu', {}).get('load_average', {}).get('1m', 0.5)
            load_averages.append(cpu_load_avg / cpu_cores if cpu_cores else 0.5)

        avg_queue = sum(queue_capacity) / len(queue_capacity) if queue_capacity else 10
        avg_active_bulk = sum(active_bulk_threads) / len(active_bulk_threads) if active_bulk_threads else 1
        heap_usage_ratio = used_heap / total_heap if total_heap else 0.6
        avg_cpu_utilization = sum(load_averages) / len(load_averages) if load_averages else 0.5

        # HTTP size limit (max content length)
        settings = es.cluster.get_settings(include_defaults=True)
        content_limit_str = settings['defaults'].get('http', {}).get('max_content_length', '100mb')

        size_multiplier = {"kb": 1024, "mb": 1024**2, "gb": 1024**3}
        for unit in size_multiplier:
            if content_limit_str.lower().endswith(unit):
                content_limit_bytes = int(content_limit_str.lower().replace(unit, '')) * size_multiplier[unit]
                break
        else:
            content_limit_bytes = 100 * 1024 * 1024  # fallback: 100 MB

        # Estimate record limit based on size
        max_by_size = content_limit_bytes // avg_record_size_bytes

        # Score-based chunk scaling
        score = (
            (1 - heap_usage_ratio) * 0.4 +
            (1 - avg_cpu_utilization) * 0.4 +
            (1 - min(avg_active_bulk / 8, 1)) * 0.2
        )
        chunk_by_perf = int(base_chunk * (1 + score))

        # Final chunk size bounded by content length and system state
        final_chunk = max(min_chunk, min(chunk_by_perf, max_by_size, max_chunk))

        print(f"[Chunk Estimator] Heap: {heap_usage_ratio:.2f}, CPU: {avg_cpu_utilization:.2f}, Threads: {avg_active_bulk}, Chunk: {final_chunk}")
        return final_chunk

    except Exception as e:
        print(f"[Chunk Estimator] Error: {e}, fallback to base chunk {base_chunk}")
        return base_chunk

def prepare_actions(df,index_name):
        actions = []
        try :
            for group, gropdf in df.groupby('PublishProductEntityId'):
                group_id =group
                valuedf =json.dumps(dict(zip(gropdf['AttributeCode'], gropdf['AttributeValue'].apply(convert_json).apply(clean_value)   )),indent=4)
                action = {
                        "_index": index_name,
                        "_id": group_id,
                        "_source": json.loads(valuedf)
                        }
                actions.append(action) 
            return actions 
        except Exception as e:
            logs.append({
                "status": "error",
                "type": type(e).__name__,
                "message": str(e)
            })
def update_json_with_locale_version(es,valuedf,index_name, locale_version_array,group_id):
    
    try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"ProductId": json.loads(valuedf).get("ZnodeProductId")}},
                            {"term": {"LocaleId": json.loads(valuedf).get("LocaleId")}}
                        ]
                    }
                }
            }
            response = es.search(index=index_name, body=query, size=1)
            hits = response.get('hits', {}).get('hits', [])
            
            if hits and group_id != hits[0]['_id']:
                valuedf.pop("VersionId", None)
                existing_doc = hits[0]['_source']
                group_id = hits[0]['_id']
                merged_doc = {**existing_doc, **valuedf}
                valuedf= merged_doc
            else:    
                data = json.loads(valuedf)
                mapping = {int(pair.split('-')[0]): int(pair.split('-')[1]) for pair in locale_version_array}
                locale_id = data.get("LocaleId")
                if locale_id in mapping:
                    data["VersionId"] = mapping[locale_id]   
                valuedf=json.dumps(data)             
    except Exception as e:
            logs.append({
                "status": "error",
                "type": type(e).__name__+"error in preview",
                "message": str(e)
            })
    return valuedf, group_id 


def prepare_updateactions(es,df,index_name,versionupdate,preview=0):
        actions = []
        try:
            for group, gropdf in df.groupby('PublishProductEntityId'):
                group_id =group
                valuedf =json.dumps(dict(zip(gropdf['AttributeCode'], gropdf['AttributeValue'].apply(convert_json).apply(clean_value)   )),indent=4)                
                action = {
                        "_op_type": "update",
                        "_index": index_name,
                        "_id": group_id,
                        "doc": json.loads(valuedf),
                        "doc_as_upsert": True
                        }
                actions.append(action) 
                if preview == 1: 
                        valuedf,group_id = update_json_with_locale_version(es,valuedf,index_name+"_preview",versionupdate,group_id)
                        action = {
                                "_op_type": "update",
                                "_index": index_name+"_preview",
                                "_id": group_id,
                                "doc": json.loads(valuedf),
                                "doc_as_upsert": True
                                }
                        actions.append(action)  
            return actions 
        except Exception as e:
            logs.append({
                "status": "error",
                "type": type(e).__name__+"error in prepare",
                "message": str(e)
            })   
def upsert_by_product_locale(es,df,index_name):
    try:
            for group, gropdf in df.groupby('PublishProductEntityId'):
                group_id =group
                valuedf =json.loads(json.dumps(dict(zip(gropdf['AttributeCode'], gropdf['AttributeValue'].apply(convert_json).apply(clean_value)   )),indent=4))
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"ZnodeProductId": valuedf.get("ZnodeProductId")}},
                                {"term": {"LocaleId": valuedf.get("LocaleId")}}
                            ]
                        }
                    }
                }
                res = es.search(index=index_name, body=query, size=1)
                if res["hits"]["hits"]:
                    valuedf.pop("PublishProductEntityId", None)
                    existing_doc_id = res["hits"]["hits"][0]["_id"]
                    # print(group,index_name,existing_doc_id,valuedf)
                    es.update(index=index_name, id=existing_doc_id,  body={"doc": valuedf,"doc_as_upsert": True })
                else:                    
                    es.index(index=index_name, document=valuedf)
    except Exception as e:
            logs.append({
                "status": "error",
                "type": type(e).__name__,
                "message": str(e)
            })     
    return True                       
def index_bulk_parallel(actions_chunk, es, chunk_size=500):
    try:
        success_count = 0
        error_count = 0
        
        for ok, item in helpers.parallel_bulk(
            client=es,
            actions=actions_chunk,
            chunk_size=chunk_size,
            thread_count=4,
            queue_size=4,
            raise_on_error=False,
            raise_on_exception=False
        ):
            if ok:
                success_count += 1
            else:
                for op_type, info in item.items():
                    error_count += 1
                    logs.append({
                        "status": "error",
                        "type": "message",
                        "message": str(info)
                    })

        return f"Success: {success_count}, Failed: {error_count}"

    except (NotFoundError, TransportError, ApiError, Exception) as e:
        logs.append({
            "status": "error",
            "type": type(e).__name__,
            "message": str(e)
        })
        return f"Exception: {e}"  
def build_update_by_query_body(mapping_list):
    script_lines = []
    locale_ids = []
    for i, pair in enumerate(mapping_list):
        try:
            locale_id, version_id = map(int, pair.split("-"))
            locale_ids.append(locale_id)
            prefix = "if" if i == 0 else "else if"
            condition = (
                f"{prefix} (ctx._source.containsKey('LocaleId') && ctx._source.LocaleId == {locale_id}L) "
                f"{{ ctx._source.VersionId = {version_id}L; }}"
            )
            script_lines.append(condition)
        except ValueError:
            raise ValueError(f"Invalid pair: '{pair}'. Format must be 'LocaleId-VersionId'.")

    painless_script = " ".join(script_lines)

    body = {
        "script": {
            "source": painless_script,
            "lang": "painless"
        },
        "query": {
            "terms": {
                "LocaleId": locale_ids
            }
        }
    }

    return body
def createelasticindex(es,indexname,indexbody):
        try:
           if not es.indices.exists(index=indexname):
                try:
                    es.indices.create(index=indexname, body=indexbody)
                except BadRequestError as e:
                    logs.append({
                        "status": "error",
                        "type": "BadRequestError",
                        "message": str(e.info if hasattr(e, "info") else str(e))
                    })
                except ConnectionError as e:
                    logs.append({
                        "status": "error",
                        "type": "ConnectionError",
                        "message": str(e)
                    })

                except TransportError as e:
                    logs.append({
                        "status": "error",
                        "type": "TransportError",
                        "message": str(e.info)
                    })

        except Exception as e:
            logs.append({
                "status": "error",
                "type": "UnexpectedError",
                "message": str(e)
            })
        except Exception as e:
            logs.append({
                "status": "error",
                "type": "OuterError",
                "message": str(e)
            })

        return True    
def processdraftproduct (host,ValueData,index_name,preview,lastexecutionofloop= 0,updatedversion="",avgrecordsizebytes=0,ispreviewonly= 0 ):
        try :
            es = Elasticsearch(hosts=host,timeout=600,max_retries=5,retry_on_timeout=True) 
            
            if es.indices.exists(index=index_name):
                    preview_indexname = index_name+"_preview"
                    es.indices.put_settings(index=index_name,body={"settings": {"index.blocks.write": False,"index.lifecycle.name": None}})
                    if preview ==1 : 
                        if es.indices.exists(index=preview_indexname) :
                              es.indices.put_settings(index=preview_indexname,body={"settings": {"index.blocks.write": False,"index.lifecycle.name": None}})
                        else :
                           logs.append({
                            "status": "error",
                            "type": "indexnotfound",
                            "message": preview_indexname+" index not exists "
                            }) 
                           return logs
                    if ispreviewonly==1:
                            upsert_by_product_locale(es,ValueData,index_name)
                    else : 
                        updateversionids=[]
                        if   updatedversion:     
                             updateversionids = updatedversion.split(",")
                        chunk_size = get_dynamic_chunk_size(es,avgrecordsizebytes)  
                        print(chunk_size) 
                        with ThreadPoolExecutor(max_workers=6) as executor:
                            futures = []
                            for chunk in chunkify(prepare_updateactions(es,ValueData,index_name,updateversionids,preview), chunk_size):
                                futures.append(executor.submit(index_bulk_parallel, chunk, es, chunk_size))
                            for future in as_completed(futures):
                                result = future.result()                              
                    if lastexecutionofloop ==1 :    
                        es.indices.put_settings(index=index_name,body={"settings": {"index.blocks.write": True,"index.lifecycle.name": "Merge-segment-800"}})
                        es.indices.refresh(index=index_name)                  
                        if preview ==1 : 
                            es.indices.put_settings(index=preview_indexname,body={"settings": {"index.blocks.write": True,"index.lifecycle.name": "Merge-segment-800"}})
                            es.indices.refresh(index=preview_indexname)            
                        logs.append({
                            "status": "sucess",
                            "type": "completed",
                            "message": "completed successfully "
                        })
            else :
                logs.append({
                        "status": "error",
                        "type": "index",
                        "message": index_name+" index not exists"
                    })        
        except Exception as e:
            logs.append({
                "status": "error",
                "type": "OuterError processdraftproduct",
                "message": str(e)
            })
        return logs  


def processindexdata (host,ValueData,index_name,oldIndexname,indexbody="",preview=0,firstexecutionofloop = 0,lastexecutionofloop = 0,updatedversion="",avgrecordsizebytes = 0,timestamp= "") : 
        try :
            es = Elasticsearch(hosts=host,timeout=600,max_retries=5,retry_on_timeout=True) 
            create_policy(es)
            if firstexecutionofloop==1:
                matching_indices = list(es.indices.get_alias(index=oldIndexname+"*").keys())
                es.indices.put_mapping(index=",".join(matching_indices) , body={"_meta": {"marker": "can-be-deleted"}})
                createelasticindex(es,index_name,indexbody)
            
            chunk_size = get_dynamic_chunk_size(es,avgrecordsizebytes)   
            with ThreadPoolExecutor(max_workers=6) as executor:
                 futures = []
                 for chunk in chunkify(prepare_actions(ValueData,index_name), chunk_size):
                     futures.append(executor.submit(index_bulk_parallel, chunk, es, chunk_size))
                 for future in as_completed(futures):
                     result = future.result()  
            
            if lastexecutionofloop ==1 :    
                 index_list= [index_name] 
                 if preview == 1 :
                        previewindexname = index_name[:-12]+"_preview"+timestamp
                        es.indices.put_settings(index=index_name,body={"settings": {"index.blocks.write": True}})
                        es.indices.clone(index=index_name,target=previewindexname,body={"settings": {"index.blocks.write": False,"number_of_replicas": 0}})
                        es.indices.put_settings(index=previewindexname,body={"settings": {"index.lifecycle.name": "Merge-segment-800"}})
                        updateversionids = updatedversion.split(",")
                        updatebody = build_update_by_query_body(updateversionids) 
                        es.indices.refresh(index=previewindexname)                  
                        es.update_by_query(index=previewindexname, body=updatebody,wait_for_completion=False, slices=2, requests_per_second=-1, refresh=True,conflicts="proceed") 
                        index_list.append(previewindexname) 
                  
                 actions = {  "actions": [] }
                 for index in index_list:
                    actions["actions"].append({
                             "add": {"index": index,"alias": index[:-12]}
                                         })
                 es.indices.update_aliases(body=actions)
                 indices_del = [i for i, m in es.indices.get_mapping().items()
                             if m["mappings"].get("_meta", {}).get("marker") == "can-be-deleted"
                                ]
                 [es.indices.delete(index=i) for i in indices_del]
                 es.indices.put_settings(index=index_name,body={"settings": {"index.blocks.write": True,"index.lifecycle.name": "Merge-segment-800"}})
                 es.indices.refresh(index=index_name)                  
            logs.append({
                 "status": "sucess",
                 "type": "completed",
                 "message": "completed successfully "
             })
        except Exception as e:
            logs.append({
                "status": "error",
                "type": "OuterError processindexdata",
                "message": str(e)
            })
        return logs  
def productmultiindex (host,valuedata):
    try:  
        es = Elasticsearch(hosts=host,timeout=600,max_retries=5,retry_on_timeout=True) 
        index_names = valuedata['IndexName'].dropna().unique().tolist()
        index_str = ",".join(index_names)
        es.indices.put_settings(index=index_str,body={"settings": {"index.blocks.write": False}})
        all_actions=prepare_bulk_productmultiindex (valuedata,es)
        print(all_actions)
        avg_record_size_bytes = calculate_avg_record_size(all_actions)
        chunk_size = get_dynamic_chunk_size(es,avg_record_size_bytes)
        with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for chunk in chunkify(all_actions, chunk_size):
                    futures.append(executor.submit(index_bulk_parallel, chunk, es, chunk_size))
                for future in as_completed(futures):
                    result = future.result()
        es.indices.put_settings(index=index_str,body={"settings": {"index.blocks.write": True}})
        logs.append({
                "status": "sucess",
                "type": "completed",
                "message": "completed successfully "
            })
    except Exception as e :
         logs.append({
                "status": "error",
                "type": "OuterError",
                "message": str(e)
            })
    return logs   
def prepare_bulk_productmultiindex(df, es):
    actions = []

    # Grouping by product ID and index name
    grouped = df.groupby(["PublishProductEntityId", "IndexName"])

    for (PublishProductEntityId, index_name), group in grouped:
        # Check if index exists
        if not es.indices.exists(index=index_name):
            logs.append({
                "status": "info",
                "type": "NotFound",
                "message": f"{index_name} not found"
            })
            continue  # Skip this group if index doesn't exist

        # Build document by converting and cleaning values
        doc = {}
        for _, row in group.iterrows():
            attr_code = row['AttributeCode']
            attr_value = row['AttributeValue']
            try:
                cleaned_value = clean_value(convert_json(attr_value))
                doc[attr_code] = cleaned_value
            except Exception as e:
                logs.append({
                    "status": "error",
                    "type": "TransformError",
                    "message": f"Error processing {attr_code}: {str(e)}"
                })
        
        # Prepare Elasticsearch action
        action = {
            "_op_type": "update",
            "_index": index_name,
            "_id": str(PublishProductEntityId),
            "doc": doc,
            "doc_as_upsert": True
        }

        actions.append(action)

    return actions
def deleteindexprodct(host,ValueData,index_name,isprod=0):
        es = Elasticsearch(hosts=host,timeout=600,max_retries=5,retry_on_timeout=True)
        indexlist =[index_name]
        if es.indices.exists(index=index_name):
            try:
                if isprod==1:
                    if es.indices.exists(index=index_name.replace("_preview","")):
                        indexlist.append(index_name.replace("_preview",""))
                    else:
                       logs.append({
                        "status": "error",
                        "type": "NotFound",
                        "message": "For delete product index not found "+index_name.replace("_preview","")
                       })    
                index_str = ",".join(indexlist) 
                id_list = ValueData['ZnodeProductId'].dropna().astype(str).tolist() 
                query = {
                        "query": {
                             "terms":{ 
                            "ZnodeProductId": id_list
                            }
                        }
                    }
                es.indices.put_settings(index=index_str,body={"settings": {"index.blocks.write": False}})    
                response = es.delete_by_query(
                                index=index_str,
                                body=query,
                                refresh=True,
                                wait_for_completion=True,
                                conflicts="proceed"
                            )
                es.indices.put_settings(index=index_str,body={"settings": {"index.blocks.write": True}})  
                logs.append({
                    "status": "sucess",
                    "type": "completed",
                    "message": "completed successfully "
                })
            except Exception as e:
                logs.append({
                    "status": "error",
                    "type": "OuterError",
                    "message": str(e)
                })
        else :
            logs.append({
                        "status": "error",
                        "type": "NotFound",
                        "message": "For delete product index not found "+index_name
                    })
        return logs
def deleteindexsingleprodct(host,ValueData,index_name,isprod=0):
        es = Elasticsearch(hosts=host,timeout=600,max_retries=5,retry_on_timeout=True)
        indexlist =[index_name]
        index_str = ",".join(indexlist) 
        if es.indices.exists(index=index_str):
            try:
                id_list = ValueData['ZnodeProductId'].dropna().astype(str).tolist() 
                query = {
                        "query": {
                             "terms":{ 
                            "ZnodeProductId": id_list
                            }
                        }
                    }
                es.indices.put_settings(index=index_str,body={"settings": {"index.blocks.write": False}})    
                response = es.delete_by_query(
                                index=index_str,
                                body=query,
                                refresh=True,
                                wait_for_completion=True,
                                conflicts="proceed"
                            )
                es.indices.put_settings(index=index_str,body={"settings": {"index.blocks.write": True}})  
                logs.append({
                    "status": "sucess",
                    "type": "completed",
                    "message": "completed successfully "
                })
            except Exception as e:
                logs.append({
                    "status": "error",
                    "type": "OuterError",
                    "message": str(e)
                })
        else :
            logs.append({
                        "status": "error",
                        "type": "NotFound",
                        "message": "For delete product index not found "+index_name
                    })
        return logs





