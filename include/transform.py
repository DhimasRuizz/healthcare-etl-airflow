import pandas as pd

def transform_data():
    df = pd.read_csv("/tmp/raw_healthcare_data.csv")

    # Data Cleaning: Remove duplicates and handle missing values
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)

    df.to_csv("/tmp/clean_healthcare_data.csv", index=False)
    print("Transform . . .")
    print("Data transformed successfully to tmp!")