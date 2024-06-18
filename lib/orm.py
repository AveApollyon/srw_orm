"""
orm.py

Этот модуль содержит классы и функции для работы с базой данных PostgreSQL,
создания и управления таблицами (Patient, Doctor, Appointment),
генерации данных и выполнения операций резервного копирования и восстановления данных.

Classes:
    DataBase: Класс для работы с базой данных PostgreSQL.
    Patient: Класс, представляющий пациента.
    Doctor: Класс, представляющий врача.
    Appointment: Класс, представляющий запись на прием у врача.
    DatabaseSandbox: Класс для создания и удаления временной базы данных-песочницы.

Usage:
    Этот модуль можно использовать для создания и управления базой данных для генерации данных о пациентах,
    врачах и записях на прием, а также для тестирования операций резервного копирования и восстановления данных.
"""

import random
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
from psycopg2 import sql
import os

fake = Faker('ru_RU')

class DataBase:
    """
    Класс для работы с базой данных PostgreSQL.

    Attributes:
        dbname (str): Имя базы данных.
        user (str): Имя пользователя.
        password (str): Пароль пользователя.
        host (str): Хост базы данных.
        conn (psycopg2.connection): Соединение с базой данных PostgreSQL.
    """

    def __init__(self, dbname, user, password, host):
        """
        Инициализирует объект для работы с базой данных.

        Args:
            dbname (str): Имя базы данных.
            user (str): Имя пользователя.
            password (str): Пароль пользователя.
            host (str): Хост базы данных.
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def __enter__(self):
        """
        Возвращает сам объект при использовании в контекстном менеджере.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрывает соединение с базой данных при выходе из контекстного менеджера.
        """
        self.disconnect()

    def connect(self):
        """
        Устанавливает соединение с базой данных PostgreSQL.
        """
        if not self.conn:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)
            self.conn.autocommit = True

    def disconnect(self):
        """
        Закрывает соединение с базой данных PostgreSQL.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None):
        """
        Выполняет SQL-запрос к базе данных.

        Args:
            query (str): SQL-запрос.
            params (tuple or list): Параметры для SQL-запроса.

        Returns:
            list or None: Результат выполнения запроса.
        """
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
            else:
                return None

    def create_table(self, model_class):
        """
        Создает таблицу в базе данных.

        Args:
            model_class (class): Класс модели данных с методом get_columns().
        """
        table_name = model_class.__name__.lower()
        columns = model_class.get_columns()
        columns_str = ', '.join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        self.execute_query(query)

    def delete_all_data(self, model_class):
        """
        Удаляет все данные из указанной таблицы.

        Args:
            model_class (class): Класс модели данных.
        """
        table_name = model_class.__name__.lower()
        query_truncate = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(table_name))
        self.execute_query(query_truncate)
        self.reset_sequence(table_name)

    def replace_all_data(self, model_class, objects):
        """
        Заменяет все данные в указанной таблице новыми данными.

        Args:
            model_class (class): Класс модели данных.
            objects (list): Список объектов для замены.
        """
        table_name = model_class.__name__.lower()
        self.delete_all_data(model_class)
        self.save_objects(objects)

    def drop_table(self, model_class):
        """
        Удаляет таблицу из базы данных.

        Args:
            model_class (class): Класс модели данных.
        """
        table_name = model_class.__name__.lower()
        query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name))
        self.execute_query(query)

    def backup_table(self, model_class, backup_path):
        """
        Создает резервную копию таблицы в формате CSV.

        Args:
            model_class (class): Класс модели данных.
            backup_path (str): Путь для сохранения резервной копии.

        Returns:
            str: Путь к файлу резервной копии.
        """
        table_name = model_class.__name__.lower()
        query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(sql.Identifier(table_name))
        backup_file = os.path.abspath(backup_path)

        with open(backup_file, 'w', encoding='utf-8') as f:
            with self.conn.cursor() as cur:
                cur.copy_expert(query, file=f)

        return backup_file

    def restore_table(self, model_class, backup_file):
        """
        Восстанавливает данные таблицы из файла резервной копии.

        Args:
            model_class (class): Класс модели данных.
            backup_file (str): Путь к файлу резервной копии.
        """
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
        """
        Сбрасывает счетчик последовательности таблицы после удаления данных.

        Args:
            table_name (str): Имя таблицы.
        """
        if table_name == 'patient':
            self.execute_query("SELECT setval('patient_patient_id_seq', (SELECT COALESCE(MAX(patient_id)+1, 1) FROM patient), false)")
        elif table_name == 'doctor':
            self.execute_query("SELECT setval('doctor_doctor_id_seq', (SELECT COALESCE(MAX(doctor_id)+1, 1) FROM doctor), false)")
        elif table_name == 'appointment':
            self.execute_query("SELECT setval('appointment_appointment_id_seq', (SELECT COALESCE(MAX(appointment_id)+1, 1) FROM appointment), false)")

    def generate_objects(self, n, generator_func):
        """
        Генерирует объекты с помощью указанной функции-генератора.

        Args:
            n (int): Количество объектов для генерации.
            generator_func (function): Функция-генератор для создания объектов.

        Returns:
            list: Список сгенерированных объектов.
        """
        return generator_func(n)

    def save_objects(self, objects):
        """
        Сохраняет объекты в соответствующие таблицы базы данных.

        Args:
            objects (list): Список объектов для сохранения.
        """
        for obj in objects:
            if isinstance(obj, Patient):
                self.save_patient(obj)
            elif isinstance(obj, Doctor):
                self.save_doctor(obj)
            elif isinstance(obj, Appointment):
                self.save_appointment(obj)

    def save_patient(self, patient):
        """
        Сохраняет пациента в таблицу patient.

        Args:
            patient (Patient): Объект класса Patient для сохранения.
        """
        table_name = Patient.__name__.lower()
        query = f"INSERT INTO {table_name} (name, age, gender) VALUES (%s, %s, %s)"
        self.execute_query(query, (patient.name, patient.age, patient.gender))

    def save_doctor(self, doctor):
        """
        Сохраняет врача в таблицу doctor.

        Args:
            doctor (Doctor): Объект класса Doctor для сохранения.
        """
        table_name = Doctor.__name__.lower()
        query = f"INSERT INTO {table_name} (name, specialty) VALUES (%s, %s)"
        self.execute_query(query, (doctor.name, doctor.specialty))

    def save_appointment(self, appointment):
        """
        Сохраняет запись на прием в таблицу appointment.

        Args:
            appointment (Appointment): Объект класса Appointment для сохранения.
        """
        table_name = Appointment.__name__.lower()
        query = f"INSERT INTO {table_name} (patient_id, doctor_id, appointment_date) VALUES (%s, %s, %s)"
        self.execute_query(query, (appointment.patient_id, appointment.doctor_id, appointment.appointment_date))


class Patient:
    """
    Класс, представляющий пациента.

    Attributes:
        name (str): Имя пациента.
        age (int): Возраст пациента.
        gender (str): Пол пациента ('Male' или 'Female').
    """

    def __init__(self, name, age, gender):
        """
        Инициализирует объект пациента.

        Args:
            name (str): Имя пациента.
            age (int): Возраст пациента.
            gender (str): Пол пациента ('Male' или 'Female').
        """
        self.name = name
        self.age = age
        self.gender = gender

    @classmethod
    def get_columns(cls):
        """
        Возвращает список столбцов для создания таблицы в базе данных.

        Returns:
            list: Список строк с описанием столбцов.
        """
        return [
            'patient_id SERIAL PRIMARY KEY',
            'name VARCHAR(100) CHECK (char_length(name) > 0)',
            'age INTEGER CHECK (age >= 0 AND age <= 120)',
            "gender VARCHAR(10) CHECK (gender IN ('Male', 'Female'))"
        ]

    @classmethod
    def generate_patients(cls, n):
        """
        Генерирует список пациентов.

        Args:
            n (int): Количество пациентов для генерации.

        Returns:
            list: Список объектов класса Patient.
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
    Класс, представляющий врача.

    Attributes:
        name (str): Имя врача.
        specialty (str): Специальность врача.
    """

    def __init__(self, name, specialty):
        """
        Инициализирует объект врача.

        Args:
            name (str): Имя врача.
            specialty (str): Специальность врача.
        """
        self.name = name
        self.specialty = specialty

    @classmethod
    def get_columns(cls):
        """
        Возвращает список столбцов для создания таблицы в базе данных.

        Returns:
            list: Список строк с описанием столбцов.
        """
        return [
            'doctor_id SERIAL PRIMARY KEY',
            'name VARCHAR(100) CHECK (char_length(name) > 0)',
            'specialty VARCHAR(100) CHECK (char_length(specialty) > 0)'
        ]

    @classmethod
    def generate_doctors(cls, n):
        """
        Генерирует список врачей.

        Args:
            n (int): Количество врачей для генерации.

        Returns:
            list: Список объектов класса Doctor.
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
    Класс, представляющий запись на прием у врача.

    Attributes:
        patient_id (int): Идентификатор пациента.
        doctor_id (int): Идентификатор врача.
        appointment_date (datetime): Дата и время приема.
    """

    def __init__(self, patient_id, doctor_id, appointment_date):
        """
        Инициализирует объект записи на прием.

        Args:
            patient_id (int): Идентификатор пациента.
            doctor_id (int): Идентификатор врача.
            appointment_date (datetime): Дата и время приема.
        """
        self.patient_id = patient_id
        self.doctor_id = doctor_id
        self.appointment_date = appointment_date

    @classmethod
    def get_columns(cls):
        """
        Возвращает список столбцов для создания таблицы в базе данных.

        Returns:
            list: Список строк с описанием столбцов.
        """
        return [
            'appointment_id SERIAL PRIMARY KEY',
            'patient_id INTEGER REFERENCES patient(patient_id) ON DELETE CASCADE',
            'doctor_id INTEGER REFERENCES doctor(doctor_id) ON DELETE CASCADE',
            'appointment_date TIMESTAMP'
        ]

    @classmethod
    def generate_appointments(cls, db, n):
        """
        Генерирует список записей на прием.

        Args:
            db (DataBase): Объект для доступа к базе данных.
            n (int): Количество записей на прием для генерации.

        Returns:
            list: Список объектов класса Appointment.
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
    """
    Контекстный менеджер для создания и управления песочницей базы данных на основе исходной базы данных.

    Этот класс облегчает создание песочницы базы данных на основе исходной,
    что позволяет безопасно экспериментировать и тестировать без влияния на оригинальные данные.

    Атрибуты:
        source_db_params (dict): Параметры подключения к исходной базе данных.
        sandbox_db_params (dict): Параметры подключения к базе данных песочницы.
        conn (psycopg2.extensions.connection): Объект соединения с базой данных песочницы.

    Методы:
        __init__(self, source_db_params, sandbox_db_params):
            Инициализирует объект песочницы базы данных с заданными параметрами подключения.

        __enter__(self):
            Создает песочницу базы данных при входе в контекстное окружение и устанавливает соединение с песочницей.

        __exit__(self, exc_type, exc_val, exc_tb):
            Закрывает соединение с песочницей и удаляет ее при выходе из контекстного окружения.

        create_sandbox(self):
            Создает новую песочницу базы данных на основе исходной базы данных.

        drop_sandbox(self):
            Удаляет песочницу базы данных.

        connect(self):
            Устанавливает соединение с базой данных песочницы.

        disconnect(self):
            Закрывает соединение с базой данных песочницы.

        execute_query(self, query, params=None):
            Выполняет SQL-запрос к базе данных песочницы с опциональными параметрами и возвращает результаты запроса.
    """
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
        """
        Создает песочницу базы данных на основе исходной базы данных.

        Этот метод подключается к исходной базе данных, завершает активные соединения
        с исходной и песочничной базами данных, удаляет существующую песочницу (если есть),
        и создает новую песочницу базы данных на основе структуры и данных исходной базы данных.
        """
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
        """
        Удаляет песочницу базы данных.

        Этот метод подключается к исходной базе данных, завершает активные соединения
        с исходной и песочничной базами данных, и удаляет песочницу базы данных.
        """
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
        """
        Устанавливает соединение с базой данных песочницы.

        Если соединение еще не установлено, метод устанавливает новое соединение
        с базой данных песочницы и включает режим автокоммита для этого соединения.
        """
        if not self.conn:
            self.conn = psycopg2.connect(dbname=self.sandbox_db_params['dbname'],
                                         user=self.sandbox_db_params['user'],
                                         password=self.sandbox_db_params['password'],
                                         host=self.sandbox_db_params['host'])
            self.conn.autocommit = True

    def disconnect(self):
        """
        Закрывает соединение с базой данных песочницы.

        Если соединение установлено, метод закрывает соединение с базой данных песочницы.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None):
        """
        Выполняет SQL-запрос к базе данных песочницы.

        Аргументы:
            query (str): SQL-запрос для выполнения.
            params (tuple or list, optional): Параметры запроса (если требуется).

        Возвращает:
            list or None: Результаты выполненного запроса (если есть).
        """
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

        new_patients = db.generate_objects(20000, Patient.generate_patients)
        db.replace_all_data(Patient, new_patients)
        print(f"Все данные в таблице 'patients' были заменены.")

        new_doctors = db.generate_objects(20000, Doctor.generate_doctors)
        db.replace_all_data(Doctor, new_doctors)

        new_appointments = Appointment.generate_appointments(db, 20000)
        db.replace_all_data(Appointment, new_appointments)

        print("Данные успешно восстановлены.")