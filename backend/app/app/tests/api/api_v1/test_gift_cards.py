import os

import pytest
from fastapi.testclient import TestClient

from app.core.config import settings

testdata = (
    ("../../data/input1.csv", "Massoub gift card", 5.0),
    ("../../data/input2.csv", "Shakshuka gift card", 6),
)


@pytest.mark.parametrize("file,expected_top,expected_rating", testdata)
def test_get_best_card(
    file, expected_top, expected_rating, client: TestClient, test_loc
) -> None:
    filename = os.path.join(test_loc, file)
    response = client.post(
        f"{settings.API_V1_STR}/gift_cards/best_product",
        files={"file": ("filename", open(filename, "rb"))},
    )
    response_json = response.json()
    assert response.status_code == 200
    assert response_json["top_product"] == expected_top
    assert response_json["product_rating"] == expected_rating


def test_emtpy_file(client: TestClient, test_loc) -> None:
    filename = os.path.join(test_loc, "../../data/empty.csv")

    response = client.post(
        f"{settings.API_V1_STR}/gift_cards/best_product",
        files={"file": ("filename", open(filename, "rb"))},
    )
    response_json = response.json()
    assert response.status_code == 400
    assert response_json["detail"] == "Empty rows are not allowed"


def test_wrong_file_format(client: TestClient, test_loc) -> None:
    filename = os.path.join(test_loc, "../../data/test.json")
    response = client.post(
        f"{settings.API_V1_STR}/gift_cards/best_product",
        files={"file": ("filename", open(filename, "rb"))},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "The csv file should have only 3 columns"


def test_wrong_number_format(client: TestClient, test_loc) -> None:
    filename = os.path.join(test_loc, "../../data/invalid-numbers.csv")
    response = client.post(
        f"{settings.API_V1_STR}/gift_cards/best_product",
        files={"file": ("filename", open(filename, "rb"))},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Not valid number format"


def test_async_task(client: TestClient, test_loc) -> None:
    filename = os.path.join(test_loc, "../../data/input1.csv")
    response = client.post(
        f"{settings.API_V1_STR}/gift_cards/best_product",
        files={"file": ("filename", open(filename, "rb"))},
        params={"async_processing": True},
    )
    response_json = response.json()
    assert response.status_code == 201
    assert "task-id" in response_json
    assert response_json["task-status"] != "FAILURE"

    response = client.get(
        f"{settings.API_V1_STR}/gift_cards/best_product/{response_json['task-id']}"
    )
    result = response.json()["result"]
    assert result["top_product"] == "Massoub gift card"
    assert result["product_rating"] == 5.0


def test_async_task_with_empty_file(client: TestClient, test_loc) -> None:
    filename = os.path.join(test_loc, "../../data/empty.csv")
    response = client.post(
        f"{settings.API_V1_STR}/gift_cards/best_product",
        files={"file": ("filename", open(filename, "rb"))},
        params={"async_processing": True},
    )
    response_json = response.json()
    assert response.status_code == 201
    assert "task-id" in response_json
    assert response_json["task-status"] != "SUCCESS"

    response = client.get(
        f"{settings.API_V1_STR}/gift_cards/best_product/{response_json['task-id']}"
    )
    result = response.json()["result"]
    assert not result
