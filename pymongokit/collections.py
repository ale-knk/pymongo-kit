# bike_rental_core/database/collections/base.py
from bson import ObjectId
import pandas as pd
from typing import Optional, Union, Callable, Any
from bike_rental_db.error_handler import BaseErrorHandler

class BaseCollection:
    def __init__(self, collection, document_model):
        self.collection = collection
        self.document_model = document_model
        self.error_handler = BaseErrorHandler()

    @staticmethod
    def _execute_with_error_handling(func: Callable, error_handler: BaseErrorHandler, *args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_handler.handle_error(e)

    def _validate_docs(self, docs: list[dict]) -> list[dict]:
        def aux_func(docs):
            return [self.document_model(**doc) for doc in docs]
         
        return self._execute_with_error_handling(
            func = aux_func,
            error_handler=self.error_handler,
            docs=docs
        )
    
    def _execute_find(self, 
                       query: dict, 
                       pipeline_steps: list = []) -> list[dict]:
        def find_agg(query: dict, pipeline_steps: list):
            pipeline = [{'$match': query}]
            if pipeline_steps:
                pipeline += pipeline_steps
            return list(self.collection.aggregate(pipeline))
        
        return self._execute_with_error_handling(
            func=find_agg,
            error_handler=self.error_handler,
            query=query,
            pipeline_steps=pipeline_steps
        )

    def _execute_insertion(self, func: Callable, docs: Union[dict, list[dict]], find_query: dict) -> Union[Optional[dict], list[dict]]:
        def insert_documents(docs: Union[dict, list[dict]]):
            if isinstance(docs, dict):
                validated_doc = self.document_model(**docs)
                result = func([validated_doc.dict()])
            else:
                validated_docs = self._validate_docs(docs)
                result = func([doc.dict() for doc in validated_docs])
            return result
        
        return self._execute_with_error_handling(
            func=insert_documents,
            error_handler=self.error_handler,
            docs=docs
        )

    def _execute_update(self, func, query: dict, update_data: dict):
        result = self.execute_with_handling(
            func=func,
            error_handler=self.error_handler,
            query=query,
            update={'$set': update_data}
        )
        if result.matched_count > 0:
            return self.find_one(query) if func == self.collection.update_one else self.find(query)
        return None if func == self.collection.update_one else []

    def _execute_delete(self, func, query: dict):
        result = self._execute_with_error_handling(
            func=func,
            error_handler=self.error_handler,
            filter=query
        )
        return result
    
    def _handle_result(self, 
                       result: list[dict], 
                       func: Callable | None = None,
                       validate: bool = True) -> Union[list[dict], pd.DataFrame]:
        if validate:
            validated_docs = self._validate_docs(result)
        if func:
            validated_docs = func(validated_docs)
            return validated_docs
        return result

    def find(self, 
             query: dict, 
             pipeline_steps: list = [],
             handle_func: Callable|None = None) -> Union[list[dict], pd.DataFrame]:
        return self._handle_result(
            result = self._execute_find(query=query, pipeline_steps=pipeline_steps),
            func=handle_func,
            validate=False)

    def find_by_id(self, 
                   document_id: ObjectId, 
                   pipeline_steps: list[dict] = None,
                   handle_func: Callable|None = None) -> Optional[dict]:
        return self.find(
            query={"_id": document_id},
            pipeline_steps=pipeline_steps,
            handle_func=handle_func,
        )
        
    def insert_many(self, docs: list[dict]) -> list[dict]:
        return self._execute_insertion(
            func=self.collection.insert_many, 
            docs=docs, 
            find_query={"_id": {"$in": [ObjectId() for _ in docs]}}
        )
    
    def insert_one(self, doc: dict) -> Optional[dict]:
        return self._execute_insertion(
            func=self.collection.insert_many, 
            docs=doc, 
            find_query={"_id": {"$in": [ObjectId()]}}
        )

    def insert_by_id(self, document_id: ObjectId, document_data: dict) -> Optional[dict]:
        document_data["_id"] = document_id
        return self.insert_one(document_data)
        
    def update_one(self, query: dict, update_data: dict) -> Optional[dict]:
        return self._execute_update(
            func=self.collection.update_one, 
            query=query, 
            update_data=update_data
        )

    def update_many(self, query: dict, update_data: dict) -> list[dict]:
        return self._execute_update(
            func=self.collection.update_many, 
            query=query, 
            update_data=update_data
        )

    def update_by_id(self, document_id: ObjectId, update_data: dict) -> Optional[dict]:
        return self.update_one({"_id": document_id}, update_data)
    
    def delete_one(self, query: dict) -> int:
        return self._execute_delete(
            func=self.collection.delete_one, 
            query=query
        )

    def delete_many(self, query: dict) -> int:
        return self._execute_delete(
            func=self.collection.delete_many, 
            query=query
        )

    def delete_by_id(self, document_id: ObjectId) -> int:
        return self.delete_one({"_id": document_id})