import csv
import random
import datetime
import subprocess
import psycopg2
from orm import (
    db_params, sandbox_db_params, generate, add_entries,
    session, sandbox_session, Patient, Doctor, Specialty, Appointment
)

def get_connection(params):
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def create_tables(params):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Patients (
            patient_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            dob DATE,
            gender VARCHAR(10)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Doctors (
            doctor_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            specialty VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Appointments (
            appointment_id SERIAL PRIMARY KEY,
            patient_id INT REFERENCES Patients(patient_id) ON DELETE CASCADE,
            doctor_id INT REFERENCES Doctors(doctor_id) ON DELETE CASCADE,
            appointment_date DATE,
            appointment_time TIME
        )
        """
    )

    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
                print("Tables created successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error creating tables: {error}")

def generate_patients_data(n):
    first_names = ['John', 'Jane', 'Michael', 'Emma', 'William', 'Olivia']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
    genders = ['Male', 'Female']

    data = []
    for _ in range(n):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        full_name = f"{first_name} {last_name}"
        dob = datetime.date(random.randint(1950, 2010), random.randint(1, 12), random.randint(1, 28))
        gender = random.choice(genders)
        data.append((full_name, dob, gender))

    return data

def insert_patients_data(data, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                for patient in data:
                    cur.execute("INSERT INTO Patients (name, dob, gender) VALUES (%s, %s, %s)", patient)
                print(f"{len(data)} records inserted successfully into Patients table.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting data into Patients table: {error}")

def generate_doctors_data(n):
    first_names = ['Dr. John', 'Dr. Jane', 'Dr. Michael', 'Dr. Emma', 'Dr. William', 'Dr. Olivia']
    specialties = ['Cardiology', 'Pediatrics', 'Orthopedics', 'Oncology', 'Neurology']

    data = []
    for _ in range(n):
        doctor_name = random.choice(first_names)
        specialty = random.choice(specialties)
        data.append((doctor_name, specialty))

    return data

def insert_doctors_data(data, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                for doctor in data:
                    cur.execute("INSERT INTO Doctors (name, specialty) VALUES (%s, %s)", doctor)
                print(f"{len(data)} records inserted successfully into Doctors table.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting data into Doctors table: {error}")

def generate_appointments_data(n, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT patient_id FROM Patients")
                patient_ids = [row[0] for row in cur.fetchall()]

                cur.execute("SELECT doctor_id FROM Doctors")
                doctor_ids = [row[0] for row in cur.fetchall()]

                if not patient_ids or not doctor_ids:
                    raise ValueError("Patients and Doctors tables must have data before generating appointments.")

                start_date = datetime.date(2024, 1, 1)
                end_date = datetime.date(2024, 12, 31)

                data = []
                for _ in range(n):
                    patient_id = random.choice(patient_ids)
                    doctor_id = random.choice(doctor_ids)
                    appointment_date = random_date(start_date, end_date)
                    appointment_time = random_time()
                    data.append((patient_id, doctor_id, appointment_date, appointment_time))

                return data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error generating appointments data: {error}")
        return []

def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)

def random_time():
    return datetime.time(random.randint(8, 17), random.randint(0, 59))

def insert_appointments_data(data, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                for appointment in data:
                    cur.execute(
                        "INSERT INTO Appointments (patient_id, doctor_id, appointment_date, appointment_time) VALUES (%s, %s, %s, %s)",
                        appointment)
                print(f"{len(data)} records inserted successfully into Appointments table.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting data into Appointments table: {error}")

def backup_table_data(table_name, file_path, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                rows = cur.fetchall()

                with open(file_path, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([desc[0] for desc in cur.description])
                    writer.writerows(rows)

                print(f"Data from table {table_name} backed up successfully to {file_path}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error backing up data from table {table_name}: {error}")

def restore_table_data(table_name, file_path, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name}")

                with open(file_path, 'r', newline='') as file:
                    reader = csv.reader(file)
                    next(reader, None)
                    for row in reader:
                        cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join(['%s']*len(row))})", row)

                print(f"Data restored successfully into table {table_name} from {file_path}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error restoring data into table {table_name}: {error}")

def delete_all_data(table_name, params):
    try:
        with get_connection(params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name}")
                print(f"All data deleted from table {table_name}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error deleting data from {table_name} table: {error}")

def drop_sandbox():
    try:
        drop_sandbox_command = [
            'dropdb',
            '-h', sandbox_db_params['host'],
            '-U', sandbox_db_params['user'],
            sandbox_db_params['dbname']
        ]

        subprocess.run(drop_sandbox_command, check=False, text=True)

    except subprocess.CalledProcessError as e:
        print(f"Error deleting sandbox: {e}")

def create_sandbox():
    try:
        print("Делаем бэкап основной базы данных")
        dump_file = 'science_work_lab_dump.sql'
        dump_command = [
            'pg_dump',
            '-f', dump_file,
            '-h', db_params['host'],
            '-U', db_params['user'],
            '-W',
            db_params['dbname'],
        ]
        subprocess.run(dump_command, check=True)

        print("Удаляем существующую песочницу, если она есть")
        drop_sandbox()

        print("Создаем песочницу")
        create_sandbox_command = f"createdb -U {sandbox_db_params['user']} {sandbox_db_params['dbname']}"
        subprocess.run(create_sandbox_command, shell=True, check=True)

        print("Копируем данные")
        restore_command = [
            'psql',
            '-h', sandbox_db_params['host'],
            '-U', sandbox_db_params['user'],
            '-d', sandbox_db_params['dbname'],
            '-f', dump_file
        ]
        subprocess.run(restore_command, check=True, text=True)

        print("Sandbox database 'science_work_lab_sandbox' created successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error creating sandbox: {e}")
    except subprocess.TimeoutExpired:
        print("Timeout expired during database dump. Check your database size and connection.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")