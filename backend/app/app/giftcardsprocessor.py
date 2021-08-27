import codecs
import csv
# define Python user-defined exceptions
from typing import TextIO


class Error(Exception):
    """Base class for other exceptions"""


class EmptyLineError(Error):
    """Raised When CSV file contain empty line"""


class InvalidColumns(Error):
    """Raised when csv columns has a different number of columns than expected"""


class InvalidColumnFormat(Error):
    """ Raised when columns format is different thant expected """


class GiftCardProcessor:
    """ A class to process GiftCards in a csv file"""

    def __init__(self, file: TextIO, skip_header: bool = True, columns: int = 3):
        """
        Parameters
        ----------
        file : TextIO
            The file to be processed by the class
        skip_header : bool
            Indicate if the header line should be skipped
        columns : int
            Number of columns inside the csv table
        Raises
        ------
        EmptyLineError
            raised when the header line is empty this error will be raised.
        InvalidColumns
            raised when columns value is different than expected
        """
        self.csv = csv.reader(codecs.iterdecode(file, "utf-8"), delimiter=",")
        self.columns = columns

        if skip_header:
            row = self.skip_header()
            self.validate_row(row)

    def validate_row(self, row):
        """ Run validation rules into a csv row
        This will run general rules for the file
        row
            a csv row to be validated

        """
        if not row:
            raise EmptyLineError
        if len(row) != self.columns:
            raise InvalidColumns

    def skip_header(self):
        """skips a line"""
        return next(self.csv, None)

    def get_max(self, column_index: int):
        """Calculate the max row of column

        iterate over file and calculate the max value of column index

        Parameters
        ----------
        column_index : int
            The index of the file to calculate the max value.

        Raises
        ------
        EmptyLineError
            raised when the header line is empty this error will be raised.
        InvalidColumns
            raised when columns value is different than expected
        InvalidColumnFormat
            raised when columns value format is different than expected

        """
        top_row = None

        for row in self.csv:
            self.validate_row(row)
            try:
                if not top_row:
                    top_row = row
                elif float(row[2]) > float(top_row[2]):
                    top_row = row
            except ValueError:
                raise InvalidColumnFormat
        return top_row
