import pyexasol as px
import pandas as pd
from dateutil.parser import parse
import sys
from os import path


def create_statement(file_path):
    data = pd.read_csv(file_path)
    dont_check = True
    statement = "create table practice1.earthquake ("
    for c in data.columns:
        if data[c].dtype == "int64":
            statement = statement + c + " decimal, "
        elif data[c].dtype == "float64":
            statement = statement + c + " double, "
        elif data[c].dtype == "object":
            try:
                timestamp = parse(data[c][1])
            except (TypeError, ValueError):
                pass
            else:
                statement = statement + c + " timestamp, "
                dont_check = False
            try:
                if dont_check:
                    isinstance(data[c][1], str)
            except (TypeError, ValueError):
                print(TypeError)
            else:
                if dont_check:
                    statement = statement + c + " varchar(512), "

        dont_check = True

    return statement[:statement.rfind(',')] + ")"


def exec_statement(statement):
    c = px.connect(dsn='192.168.56.101:8563', user='sys', password='exasol')
    try:
        stmt = c.execute(statement)
        for row in stmt:
            print(row)
    except px.ExaRuntimeError:
        if 'Attempt to fetch from statement without result set' in str(px.ExaRuntimeError):
            pass
    return True


def main(file_path):
    if path.exists(file_path):
        print("Found the file!")
        stat = create_statement(file_path)
        print("going to execute\n\t", stat)
        if exec_statement(stat):
            print("Table created!")

    else:
        print("File", file_path, "not found! \nAre you sure about the file name?")


if __name__ == "__main__":
    try:
        main(str(sys.argv[1]))
    except IndexError:
        print("Insufficient Or Wrong Input Argument: Path to csv file is not given!")
        print("\n\tExample: python load_table.py my_data.csv")
