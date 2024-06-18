import random
import re
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
from psycopg2 import sql
import os

fake = Faker('ru_RU')

class DataBase:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)
            self.conn.autocommit = True

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
            else:
                return None

    def create_table(self, model_class):
        table_name = model_class.__name__.lower()
        columns = model_class.get_columns()
        columns_str = ', '.join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        self.execute_query(query)

    def delete_all_data(self, model_class):
        table_name = model_class.__name__.lower()
        query_truncate = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(table_name))
        self.execute_query(query_truncate)
        self.reset_sequence(table_name)

    def replace_all_data(self, model_class, objects):
        table_name = model_class.__name__.lower()
        self.delete_all_data(model_class)
        self.save_objects(objects)
        print(f"All data in the table '{table_name}' has been replaced.")

    def drop_table(self, model_class):
        table_name = model_class.__name__.lower()
        query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name))
        self.execute_query(query)

    def backup_table(self, model_class, backup_path):
        table_name = model_class.__name__.lower()
        query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(sql.Identifier(table_name))
        backup_file = os.path.abspath(backup_path)

        with open(backup_file, 'w', encoding='utf-8') as f:
            with self.conn.cursor() as cur:
                cur.copy_expert(query, file=f)

        return backup_file

    def restore_table(self, model_class, backup_file):
        table_name = model_class.__name__.lower()
        query_truncate = f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"
        self.execute_query(query_truncate)

        query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(table_name))
        backup_file = os.path.abspath(backup_file)

        with open(backup_file, 'r', encoding='utf-8') as f:
            with self.conn.cursor() as cur:
                cur.copy_expert(query, f)

        self.reset_sequence(table_name)

    def reset_sequence(self, table_name):
        if table_name == 'patient':
            self.execute_query("SELECT setval('patient_patient_id_seq', (SELECT COALESCE(MAX(patient_id)+1, 1) FROM patient), false)")
        elif table_name == 'doctor':
            self.execute_query("SELECT setval('doctor_doctor_id_seq', (SELECT COALESCE(MAX(doctor_id)+1, 1) FROM doctor), false)")
        elif table_name == 'appointment':
            self.execute_query("SELECT setval('appointment_appointment_id_seq', (SELECT COALESCE(MAX(appointment_id)+1, 1) FROM appointment), false)")

    def generate_objects(self, n, generator_func):
        return generator_func(n)

    def save_objects(self, objects):
        for obj in objects:
            if isinstance(obj, Patient):
                self.save_patient(obj)
            elif isinstance(obj, Doctor):
                self.save_doctor(obj)
            elif isinstance(obj, Appointment):
                self.save_appointment(obj)

    def save_patient(self, patient):
        table_name = Patient.__name__.lower()
        query = f"INSERT INTO {table_name} (name, age, gender) VALUES (%s, %s, %s)"
        self.execute_query(query, (patient.name, patient.age, patient.gender))

    def save_doctor(self, doctor):
        table_name = Doctor.__name__.lower()
        query = f"INSERT INTO {table_name} (name, specialty) VALUES (%s, %s)"
        self.execute_query(query, (doctor.name, doctor.specialty))

    def save_appointment(self, appointment):
        table_name = Appointment.__name__.lower()
        query = f"INSERT INTO {table_name} (patient_id, doctor_id, appointment_date) VALUES (%s, %s, %s)"
        self.execute_query(query, (appointment.patient_id, appointment.doctor_id, appointment.appointment_date))

class Patient:
    """
    Модель для хранения данных о пациентах.

    patient_id: SERIAL PRIMARY KEY
    name: VARCHAR(100) CHECK (char_length(name) > 0)
    age: INTEGER CHECK (age >= 0 AND age <= 120)
    gender: VARCHAR(10) CHECK (gender IN ('Male', 'Female'))
    """

    def __init__(self, name, age, gender):
        self.name = name
        self.age = age
        self.gender = gender

    @classmethod
    def get_columns(cls):
        """
        Возвращает список столбцов для создания таблицы в формате SQL, извлекая данные из docstring.
        """
        columns = []
        docstring = cls.__doc__
        if docstring:
            matches = re.findall(r'(\w+): (\w+ .*)', docstring, re.MULTILINE)
            for match in matches:
                field_name = match[0]
                field_type = match[1]
                column_str = f"{field_name} {field_type}"
                columns.append(column_str)
        return columns

    @classmethod
    def generate_patients(cls, n):
        """
        Генерирует список пациентов с случайными данными.
        """
        patients = []
        for _ in range(n):
            name = fake.name()
            age = random.randint(18, 100)
            gender = random.choice(['Male', 'Female'])
            patients.append(cls(name, age, gender))
        return patients

class Doctor:
    """
    Модель для хранения данных о врачах.

    doctor_id: SERIAL PRIMARY KEY
    name: VARCHAR(100) CHECK (char_length(name) > 0)
    specialty: VARCHAR(100) CHECK (char_length(specialty) > 0)
    """

    def __init__(self, name, specialty):
        self.name = name
        self.specialty = specialty

    @classmethod
    def get_columns(cls):
        """
        Возвращает список столбцов для создания таблицы в формате SQL, извлекая данные из docstring.
        """
        columns = []
        docstring = cls.__doc__
        if docstring:
            matches = re.findall(r'(\w+): (\w+ .*)', docstring, re.MULTILINE)
            for match in matches:
                field_name = match[0]
                field_type = match[1]
                column_str = f"{field_name} {field_type}"
                columns.append(column_str)
        return columns

    @classmethod
    def generate_doctors(cls, n):
        """
        Генерирует список врачей с случайными данными.
        """
        doctors = []
        specialties = ['Cardiologist', 'Dermatologist', 'Endocrinologist', 'Pediatrician', 'Neurologist']
        for _ in range(n):
            name = fake.name()
            specialty = random.choice(specialties)
            doctors.append(cls(name, specialty))
        return doctors

class Appointment:
    """
    Модель для хранения данных о записях на прием.

    appointment_id: SERIAL PRIMARY KEY
    patient_id: INTEGER REFERENCES patient(patient_id) ON DELETE CASCADE
    doctor_id: INTEGER REFERENCES doctor(doctor_id) ON DELETE CASCADE
    appointment_date: TIMESTAMP
    """

    def __init__(self, patient_id, doctor_id, appointment_date):
        self.patient_id = patient_id
        self.doctor_id = doctor_id
        self.appointment_date = appointment_date

    @classmethod
    def get_columns(cls):
        """
        Возвращает список столбцов для создания таблицы в формате SQL, извлекая данные из docstring.
        """
        columns = []
        docstring = cls.__doc__
        if docstring:
            matches = re.findall(r'(\w+): (\w+ .*)', docstring, re.MULTILINE)
            for match in matches:
                field_name = match[0]
                field_type = match[1]
                column_str = f"{field_name} {field_type}"
                columns.append(column_str)
        return columns

    @classmethod
    def generate_appointments(cls, db, n):
        """
        Генерирует список записей на прием с случайными данными.
        """
        appointments = []
        start_date = datetime.now() - timedelta(days=365)
        end_date = datetime.now()

        patient_ids = db.execute_query("SELECT patient_id FROM patient")
        doctor_ids = db.execute_query("SELECT doctor_id FROM doctor")

        patient_ids = [pid[0] for pid in patient_ids]
        doctor_ids = [did[0] for did in doctor_ids]
        for _ in range(n):
            patient_id = random.choice(patient_ids)
            doctor_id = random.choice(doctor_ids)
            appointment_date = fake.date_time_between(start_date=start_date, end_date=end_date)
            appointments.append(cls(patient_id, doctor_id, appointment_date))
        return appointments


class DatabaseSandbox:
    def __init__(self, source_db_params, sandbox_db_params):
        self.source_db_params = source_db_params
        self.sandbox_db_params = sandbox_db_params
        self.conn = None

    def __enter__(self):
        self.create_sandbox()
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        self.drop_sandbox()

    def create_sandbox(self):
        conn = psycopg2.connect(dbname=self.source_db_params['dbname'],
                                user=self.source_db_params['user'],
                                password=self.source_db_params['password'],
                                host=self.source_db_params['host'])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();"), (self.source_db_params['dbname'],))
                cur.execute(sql.SQL("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();"), (self.sandbox_db_params['dbname'],))
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(self.sandbox_db_params['dbname'])))
                cur.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(self.sandbox_db_params['dbname']),
                    sql.Identifier(self.source_db_params['dbname'])
                ))
        finally:
            conn.close()

    def drop_sandbox(self):
        conn = psycopg2.connect(dbname=self.source_db_params['dbname'],
                                user=self.source_db_params['user'],
                                password=self.source_db_params['password'],
                                host=self.source_db_params['host'])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();"), (self.sandbox_db_params['dbname'],))
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(self.sandbox_db_params['dbname'])))
        finally:
            conn.close()

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(dbname=self.sandbox_db_params['dbname'],
                                         user=self.sandbox_db_params['user'],
                                         password=self.sandbox_db_params['password'],
                                         host=self.sandbox_db_params['host'])
            self.conn.autocommit = True

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None):
        self.connect()
        with self.conn.cursor() as cur:
            if isinstance(params, list):
                for param in params:
                    cur.execute(query, param)
            else:
                cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
            else:
                return None


if __name__ == "__main__":
    fake = Faker('ru_RU')

    db_params = {
        'dbname': 'science_work_lab',
        'user': 'postgres',
        'password': '52436150',
        'host': 'localhost'
    }

    sandbox_db_params = {
        'dbname': 'science_work_lab_sandbox',
        'user': 'postgres',
        'password': '52436150',
        'host': 'localhost'
    }

    with DataBase(**db_params) as db:
        db.create_table(Patient)
        db.create_table(Doctor)
        db.create_table(Appointment)

        patients = db.generate_objects(10000, Patient.generate_patients)
        db.save_objects(patients)
        doctors = db.generate_objects(10000, Doctor.generate_doctors)
        db.save_objects(doctors)
        appointments = Appointment.generate_appointments(db, 10000)
        db.save_objects(appointments)

        patients_backup_file = db.backup_table(Patient, 'patients_backup.csv')
        doctors_backup_file = db.backup_table(Doctor, 'doctors_backup.csv')
        appointments_backup_file = db.backup_table(Appointment, 'appointments_backup.csv')

        print(f"Созданы резервные копии: {patients_backup_file}, {doctors_backup_file}, {appointments_backup_file}")

        db.delete_all_data(Patient)
        db.delete_all_data(Doctor)
        db.delete_all_data(Appointment)

        db.restore_table(Patient, patients_backup_file)
        db.restore_table(Doctor, doctors_backup_file)
        db.restore_table(Appointment, appointments_backup_file)

        new_patients = db.generate_objects(500, Patient.generate_patients)
        db.replace_all_data(Patient, new_patients)

        new_doctors = db.generate_objects(500, Doctor.generate_doctors)
        db.replace_all_data(Doctor, new_doctors)

        new_appointments = Appointment.generate_appointments(db, 500)
        db.replace_all_data(Appointment, new_appointments)

        print("Данные успешно восстановлены.")