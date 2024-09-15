from pydantic import BaseModel, Field, root_validator
from bson import ObjectId
from datetime import datetime
from typing import Optional

class BaseDocument(BaseModel):
    mongoid: Optional[str] = Field(None, description="MongoDB string ObjectID")

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
            ObjectId: lambda x: str(x)
        }

    @root_validator(pre=True)
    def handle_objectid(cls, values):
        if '_id' in values:
            values['mongoid'] = str(values.pop('_id'))
        return values

    def dict(self, *args, **kwargs):
        model_dict = super().dict(*args, **kwargs)
        return {k: v for k, v in model_dict.items() if k != 'mongoid'}