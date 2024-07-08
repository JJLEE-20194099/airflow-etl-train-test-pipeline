from pydantic import BaseModel
from enum import Enum
class MLOpsEXPData(BaseModel):
    exp_id: str

class SourceEnum(str, Enum):
    meeyland = 'meeyland'
    batdongsan = 'batdongsan'


class MLOpsFullExpData(BaseModel):
    exp_id: str | None
    num_page: int
    source: SourceEnum
    class Config:
        use_enum_values = True
