from pydantic import BaseModel
from typing import Optional

class TrackEvent(BaseModel):
    hostname: str
    browser: str
    timestamp: str
    is_bot: int
    user_cookie: str
    referer: Optional[str]
    x_forwarded_for: str
    date_num: int
    url: str
    timestamp_as_int: int
