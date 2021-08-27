from typing import Optional

from pydantic import BaseModel


class CardsProperties(BaseModel):
    """
    async_processing :bool
        run the task asynchronously
    """

    async_processing: Optional[bool] = False
