import pandas as pd

def number_of_null_values(df, column):
    null_count = df[column].isnull().sum()
    if null_count == 0:
        return True
    else:
        return False