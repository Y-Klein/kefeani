from pydantic import BaseModel,Field,EmailStr
from typing import Optional
from datetime import datetime
from uuid import uuid4,UUID




class User(BaseModel):
    id_user:UUID = Field(default_factory=lambda: uuid4())
    name_full : str = Field(min_length=2)
    email : EmailStr
    age : int = Field(le=120,ge=13)
    phone :Optional[str] = Field(default=None,min_length=9,max_length=15)
    city : Optional[str] = Field(default=None,min_length=2)
    created_at :datetime = Field(default_factory= lambda: datetime.now())