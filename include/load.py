import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_data():
    df = pd.read_csv("/tmp/clean_healthcare_data.csv")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS healthcare_data (
            id SERIAL PRIMARY KEY,
            name TEXT,
            age INT,
            gender TEXT,
            blood_type TEXT,
            medical_condition TEXT,
            date_of_admission DATE,
            doctor_name TEXT,
            hospital_name TEXT,
            insurance_provider TEXT,
            billing_amount FLOAT,
            room_number INT,
            admission_status TEXT,
            discharge_date DATE,
            medication TEXT,
            test_results TEXT
        );
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO healthcare_data
            (name, age, gender, blood_type, medical_condition, date_of_admission, doctor_name, hospital_name, insurance_provider, billing_amount, room_number, admission_status, discharge_date, medication, test_results)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,(
            row['Name'],
            row['Age'],
            row['Gender'],
            row['Blood Type'],
            row['Medical Condition'],
            row['Date of Admission'],
            row['Doctor'],
            row['Hospital'],
            row['Insurance Provider'],
            row['Billing Amount'],
            row['Room Number'],
            row['Admission Type'],
            row['Discharge Date'],
            row['Medication'],
            row['Test Results']
        ))
        
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Load . . .")
    print(f"Loaded {len(df)} records to PostgreSQL successfully!")
