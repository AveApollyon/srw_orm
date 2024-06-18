import random
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

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)
            self.conn.autocommit = True  # Включаем автоматический коммит

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else None

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

        # Manually reset sequences
        if table_name == 'patient':
            self.execute_query(
                "SELECT setval('patient_patient_id_seq', (SELECT COALESCE(MAX(patient_id)+1, 1) FROM patient), false)")
        elif table_name == 'doctor':
            self.execute_query(
                "SELECT setval('doctor_doctor_id_seq', (SELECT COALESCE(MAX(doctor_id)+1, 1) FROM doctor), false)")
        elif table_name == 'appointment':
            self.execute_query(
                "SELECT setval('appointment_appointment_id_seq', (SELECT COALESCE(MAX(appointment_id)+1, 1) FROM appointment), false)")

    def drop_table(self, model_class):
        table_name = model_class.__name__.lower()
        query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name))
        self.execute_query(query)

    def backup_table(self, model_class, backup_path):
        table_name = model_class.__name__.lower()
        query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(sql.Identifier(table_name))
        backup_file = os.path.abspath(backup_path)

        with open(backup_file, 'w', encoding='utf-8') as f:  # Указываем кодировку UTF-8
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

        # Восстановление последовательностей
        if table_name == 'patient':
            self.execute_query(
                "SELECT setval('patient_patient_id_seq', (SELECT COALESCE(MAX(patient_id)+1, 1) FROM patient), false)")
        elif table_name == 'doctor':
            self.execute_query(
                "SELECT setval('doctor_doctor_id_seq', (SELECT COALESCE(MAX(doctor_id)+1, 1) FROM doctor), false)")
        elif table_name == 'appointment':
            self.execute_query(
                "SELECT setval('appointment_appointment_id_seq', (SELECT COALESCE(MAX(appointment_id)+1, 1) FROM appointment), false)")

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

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class Patient:
    def __init__(self, name, age, gender):
        self.name = name
        self.age = age
        self.gender = gender

    @classmethod
    def get_columns(cls):
        return [
            'patient_id SERIAL PRIMARY KEY',
            'name VARCHAR(100) CHECK (char_length(name) > 0)',
            'age INTEGER CHECK (age >= 0 AND age <= 120)',
            'gender VARCHAR(10) CHECK (gender IN (\'Male\', \'Female\'))'
        ]

    @classmethod
    def generate_patients(cls, n):
        patients = []
        for _ in range(n):
            name = fake.name()
            age = random.randint(18, 100)
            gender = random.choice(['Male', 'Female'])
            patients.append(cls(name, age, gender))
        return patients


class Doctor:
    def __init__(self, name, specialty):
        self.name = name
        self.specialty = specialty

    @classmethod
    def get_columns(cls):
        return [
            'doctor_id SERIAL PRIMARY KEY',
            'name VARCHAR(100) CHECK (char_length(name) > 0)',
            'specialty VARCHAR(100) CHECK (char_length(specialty) > 0)'
        ]

    @classmethod
    def generate_doctors(cls, n):
        doctors = []
        specialties = ['Cardiologist', 'Dermatologist', 'Endocrinologist', 'Pediatrician', 'Neurologist']
        for _ in range(n):
            name = fake.name()
            specialty = random.choice(specialties)
            doctors.append(cls(name, specialty))
        return doctors


class Appointment:
    def __init__(self, patient_id, doctor_id, appointment_date):
        self.patient_id = patient_id
        self.doctor_id = doctor_id
        self.appointment_date = appointment_date

    @classmethod
    def get_columns(cls):
        return [
            'appointment_id SERIAL PRIMARY KEY',
            'patient_id INTEGER REFERENCES patient(patient_id)',
            'doctor_id INTEGER REFERENCES doctor(doctor_id)',
            'appointment_date TIMESTAMP'
        ]

    @classmethod
    def generate_appointments(cls, db, n):
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


if __name__ == "__main__":

    fake = Faker('ru_RU')

    db_params = {
        'dbname': 'science_work_lab',
        'user': 'postgres',
        'password': '52436150',
        'host': 'localhost'
    }

    db = DataBase(**db_params)

    # Создание таблиц
    db.create_table(Patient)
    db.create_table(Doctor)
    db.create_table(Appointment)

    # Генерация и сохранение данных
    patients = db.generate_objects(10000, Patient.generate_patients)
    db.save_objects(patients)
    doctors = db.generate_objects(10000, Doctor.generate_doctors)
    db.save_objects(doctors)
    appointments = Appointment.generate_appointments(db, 10000)
    db.save_objects(appointments)

    # Резервное копирование данных
    patients_backup_file = db.backup_table(Patient, 'patients_backup.csv')
    doctors_backup_file = db.backup_table(Doctor, 'doctors_backup.csv')
    appointments_backup_file = db.backup_table(Appointment, 'appointments_backup.csv')

    print(f"Созданы резервные копии: {patients_backup_file}, {doctors_backup_file}, {appointments_backup_file}")

    # Удаление всех данных
    db.delete_all_data(Patient)
    db.delete_all_data(Doctor)
    db.delete_all_data(Appointment)

    # Восстановление данных
    db.restore_table(Patient, patients_backup_file)
    db.restore_table(Doctor, doctors_backup_file)
    db.restore_table(Appointment, appointments_backup_file)

    print("Данные успешно восстановлены.")
