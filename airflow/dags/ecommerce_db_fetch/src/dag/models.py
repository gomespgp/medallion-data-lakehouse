from pydantic import BaseModel
from typing import List


class TableConfig(BaseModel):
    name: str


class EcommerceConfig(BaseModel):
    tables: List[TableConfig]


class FetchDagConfig(BaseModel):
    ecommerce: EcommerceConfig
