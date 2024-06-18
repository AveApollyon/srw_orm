import psycopg2
from psycopg2 import sql
from faker import Faker
import random
from datetime import datetime, timedelta
import os

class Database:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def __enter__(self):
        self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cur.close()
        self.conn.close()

    def execute_query(self, query, params=None):
        self.cur.execute(query, params)

    def execute_query_many(self, query, data_list):
        self.cur.executemany(query, data_list)

    def fetch_query(self, query, params=None):
        self.cur.execute(query, params)
        return self.cur.fetchall()

    def backup_table(self, table_name, backup_path):
        query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(sql.Identifier(table_name))
        backup_file = os.path.abspath(backup_path)

        with open(backup_file, 'w') as f:
            self.cur.copy_expert(query, file=f)

        return backup_file

    def restore_table(self, table_name, backup_file):
        self.execute_query(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")

        query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(table_name))
        backup_file = os.path.abspath(backup_file)

        with open(backup_file, 'r') as f:
            self.cur.copy_expert(query, f)

        if table_name == 'patients':
            self.execute_query("SELECT setval('patients_patient_id_seq', (SELECT COALESCE(MAX(patient_id)+1, 1) FROM patients), false)")
        elif table_name == 'doctors':
            self.execute_query("SELECT setval('doctors_doctor_id_seq', (SELECT COALESCE(MAX(doctor_id)+1, 1) FROM doctors), false)")
        elif table_name == 'appointments':
            self.execute_query("SELECT setval('appointments_appointment_id_seq', (SELECT COALESCE(MAX(appointment_id)+1, 1) FROM appointments), false)")

    def delete_all_data(self, table_name, cascade=False):
        if cascade:
            query = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(table_name))
        else:
            query = sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name))
        self.execute_query(query)

class Patients:
    """
    Table: patients
    Columns:
        patient_id: SERIAL PRIMARY KEY
        name: VARCHAR(100)
        age: INTEGER
        gender: VARCHAR(10)
    """
    @classmethod
    def create_table(cls, db):
        query = """
        CREATE TABLE IF NOT EXISTS patients (
            patient_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER,
            gender VARCHAR(10)
        );
        """
        db.execute_query(query)

    @classmethod
    def generate_data(cls, db, n):
        faker = Faker()
        patients_data = []

        for _ in range(n):
            name = faker.name()
            age = random.randint(18, 100)
            gender = faker.random_element(['Male', 'Female'])
            patients_data.append((name, age, gender))

        query = """
        INSERT INTO patients (name, age, gender)
        VALUES (%s, %s, %s);
        """
        db.execute_query_many(query, patients_data)

    @classmethod
    def save_generated_data(cls, db, n):
        cls.generate_data(db, n)

    @classmethod
    def delete_all_data(cls, db):
        db.delete_all_data('patients', cascade=True)
        db.execute_query("ALTER SEQUENCE patients_patient_id_seq RESTART WITH 1")

    @classmethod
    def backup_data(cls, db, backup_path):
        return db.backup_table('patients', backup_path)

    @classmethod
    def restore_data(cls, db, backup_file):
        db.restore_table('patients', backup_file)

class Doctors:
    """
    Table: doctors
    Columns:
        doctor_id: SERIAL PRIMARY KEY
        name: VARCHAR(100)
        specialty: VARCHAR(100)
    """
    @classmethod
    def create_table(cls, db):
        query = """
        CREATE TABLE IF NOT EXISTS doctors (
            doctor_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            specialty VARCHAR(100)
        );
        """
        db.execute_query(query)

    @classmethod
    def generate_data(cls, db, n):
        faker = Faker()
        specialties = ['Cardiologist', 'Dermatologist', 'Endocrinologist', 'Pediatrician', 'Neurologist']
        doctors_data = []

        for _ in range(n):
            name = faker.name()
            specialty = random.choice(specialties)
            doctors_data.append((name, specialty))
        query = """
        INSERT INTO doctors (name, specialty)
        VALUES (%s, %s);
        """
        db.execute_query_many(query, doctors_data)

    @classmethod
    def save_generated_data(cls, db, n):
        cls.generate_data(db, n)

    @classmethod
    def delete_all_data(cls, db):
        db.delete_all_data('doctors', cascade=True)
        db.execute_query("ALTER SEQUENCE doctors_doctor_id_seq RESTART WITH 1")

    @classmethod
    def backup_data(cls, db, backup_path):
        return db.backup_table('doctors', backup_path)

    @classmethod
    def restore_data(cls, db, backup_file):
        db.restore_table('doctors', backup_file)

class Appointments:
    """
    Table: appointments
    Columns:
        appointment_id: SERIAL PRIMARY KEY
        patient_id: INTEGER REFERENCES patients(patient_id)
        doctor_id: INTEGER REFERENCES doctors(doctor_id)
        appointment_date: TIMESTAMP
    """
    @classmethod
    def create_table(cls, db):
        query = """
        CREATE TABLE IF NOT EXISTS appointments (
            appointment_id SERIAL PRIMARY KEY,
            patient_id INTEGER REFERENCES patients(patient_id),
            doctor_id INTEGER REFERENCES doctors(doctor_id),
            appointment_date TIMESTAMP
        );
        """
        db.execute_query(query)

    @classmethod
    def generate_data(cls, db, n):
        faker = Faker()
        appointments_data = []
        start_date = datetime.now()
        end_date = start_date + timedelta(days=365)

        patient_ids = db.fetch_query("SELECT patient_id FROM patients")
        doctor_ids = db.fetch_query("SELECT doctor_id FROM doctors")

        if not patient_ids or not doctor_ids:
            print("No existing patients or doctors to create appointments.")
            return

        patient_ids = [pid[0] for pid in patient_ids]
        doctor_ids = [did[0] for did in doctor_ids]

        for _ in range(n):
            patient_id = random.choice(patient_ids)
            doctor_id = random.choice(doctor_ids)
            appointment_date = faker.date_time_between(start_date=start_date, end_date=end_date)
            appointments_data.append((patient_id, doctor_id, appointment_date))

        query = """
        INSERT INTO appointments (patient_id, doctor_id, appointment_date)
        VALUES (%s, %s, %s);
        """
        db.execute_query_many(query, appointments_data)

    @classmethod
    def save_generated_data(cls, db, n):
        cls.generate_data(db, n)

    @classmethod
    def delete_all_data(cls, db):
        db.delete_all_data('appointments', cascade=True)
        db.execute_query("ALTER SEQUENCE appointments_appointment_id_seq RESTART WITH 1")

    @classmethod
    def backup_data(cls, db, backup_path):
        return db.backup_table('appointments', backup_path)

    @classmethod
    def restore_data(cls, db, backup_file):
        db.restore_table('appointments', backup_file)

def create_all_tables(db_params):
    with Database(**db_params) as db:
        Patients.create_table(db)
        Doctors.create_table(db)
        Appointments.create_table(db)

if __name__ == "__main__":
    db_params = {
        'dbname': 'science_work_lab',
        'user': 'postgres',
        'password': '52436150',
        'host': 'localhost'
    }

    # Create tables
    create_all_tables(db_params)

    # Generate data
    with Database(**db_params) as db:
        Patients.save_generated_data(db, 30)
        Doctors.save_generated_data(db, 25)
        Appointments.save_generated_data(db, 20)

    # Backup data
    with Database(**db_params) as db:
        patients_backup_file = Patients.backup_data(db, 'patients_backup.csv')
        doctors_backup_file = Doctors.backup_data(db, 'doctors_backup.csv')
        appointments_backup_file = Appointments.backup_data(db, 'appointments_backup.csv')

    print(f"Backups created: {patients_backup_file}, {doctors_backup_file}, {appointments_backup_file}")

    # Delete all data
    with Database(**db_params) as db:
        Patients.delete_all_data(db)
        Doctors.delete_all_data(db)
        Appointments.delete_all_data(db)

    # Restore data
    with Database(**db_params) as db:
        Patients.restore_data(db, patients_backup_file)
        Doctors.restore_data(db, doctors_backup_file)
        Appointments.restore_data(db, appointments_backup_file)

    print("Data restored successfully.")
