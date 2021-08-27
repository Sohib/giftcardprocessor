import os

import pytest

from app.giftcardsprocessor import EmptyLineError, InvalidColumnFormat, InvalidColumns
from app.worker import get_best_gift_card


def test_get_best_card(test_loc):
    filename = os.path.join(test_loc, "../data/input1.csv")
    task = get_best_gift_card.s(csvfile=str(filename)).apply()
    assert task.state == "SUCCESS"
    assert task.result["top_product"] == "Massoub gift card"
    assert task.result["product_rating"] == 5.0


def test_empty_file(test_loc):
    filename = os.path.join(test_loc, "../data/empty.csv")
    task = get_best_gift_card.s(csvfile=str(filename)).apply()

    assert task.state == "FAILURE"
    with pytest.raises(EmptyLineError):
        task.get()


def test_wrong_file_format(test_loc):
    filename = os.path.join(test_loc, "../data/test.json")
    task = get_best_gift_card.s(csvfile=str(filename)).apply()

    assert task.state == "FAILURE"
    with pytest.raises(InvalidColumns):
        task.get()


def test_wrong_number_format(test_loc):
    filename = os.path.join(test_loc, "../data/invalid-numbers.csv")
    task = get_best_gift_card.s(csvfile=str(filename)).apply()

    assert task.state == "FAILURE"
    with pytest.raises(InvalidColumnFormat):
        task.get()
