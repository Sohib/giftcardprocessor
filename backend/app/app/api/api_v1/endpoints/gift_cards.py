import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

from celery.result import AsyncResult
from fastapi import APIRouter, File, HTTPException, Response, UploadFile, status

from app.core.celery_app import celery_app
from app.core.config import settings
from app.giftcardsprocessor import (
    EmptyLineError,
    GiftCardProcessor,
    InvalidColumnFormat,
    InvalidColumns,
)

router = APIRouter()


def save_upload_file_tmp(upload_file: UploadFile) -> Path:
    """ saves uploaded file to temp_path"""
    try:
        suffix = Path(upload_file.filename).suffix
        with NamedTemporaryFile(
            delete=False, suffix=suffix, dir=settings.UPLOAD_DIR
        ) as tmp:
            shutil.copyfileobj(upload_file.file, tmp)
            tmp_path = Path(tmp.name)
    finally:
        upload_file.file.close()
    return tmp_path


@router.post("/best_product")
def get_best_product(
    response: Response, async_processing: bool = False, file: UploadFile = File(...)
):
    if async_processing:
        tmp_file = save_upload_file_tmp(file)
        task: AsyncResult = celery_app.send_task(
            "app.worker.get_best_gift_card", args=(str(tmp_file),)
        )
        response.status_code = status.HTTP_201_CREATED
        return {"task-id": task.task_id, "task-status": task.state}

    try:
        gift_cards_processor = GiftCardProcessor(file.file,)
        max_row = gift_cards_processor.get_max(3)
        return {"top_product": max_row[1], "product_rating": float(max_row[2])}
    except EmptyLineError:
        raise HTTPException(status_code=400, detail="Empty rows are not allowed")
    except InvalidColumns:
        raise HTTPException(
            status_code=400, detail="The csv file should have only 3 columns"
        )
    except InvalidColumnFormat:
        raise HTTPException(status_code=400, detail="Not valid number format")


@router.get("/best_product/{task_id}")
def get_best_product(task_id: str):
    task = AsyncResult(task_id, backend=celery_app.backend)

    return {"task-id": task.task_id, "task-status": task.state, "result": task.result}
