import re

import psycopg2
import random
import datetime
from psycopg2 import sql


class Field:
    def __init__(self, column_type, primary_key=False, foreign_key=None, max_length=None, min_value=None):
        self.column_type = column_type
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.max_length = max_length
        self.min_value = min_value


class IntegerField(Field):
    def __init__(self, primary_key=False, foreign_key=None, min_value=None):
        super().__init__("INT", primary_key, foreign_key, min_value=min_value)


class StringField(Field):
    def __init__(self, primary_key=False, foreign_key=None, max_length=None):
        super().__init__("VARCHAR", primary_key, foreign_key, max_length=max_length)


class ForeignKey:
    def __init__(self, ref_table):
        self.ref_table = ref_table


def parse_docstring(docstring):
    fields = {}
    for line in docstring.strip().split('\n'):
        match = re.match(r'(\w+):\s*(\w+)\((.*)\)', line.strip())
        if match:
            name, field_type, params = match.groups()
            params_dict = {}
            for param in params.split(','):
                key, value = param.split('=')
                key = key.strip()
                value = value.strip().strip("'").strip('"')
                if key == 'primary_key':
                    params_dict[key] = value.lower() == 'true'
                elif key == 'foreign_key':
                    params_dict[key] = ForeignKey(value)
                elif key == 'max_length':
                    params_dict[key] = int(value)
                elif key == 'min_value':
                    params_dict[key] = int(value)
            if field_type == 'CharField':
                fields[name] = StringField(**params_dict)
            elif field_type == 'IntegerField':
                fields[name] = IntegerField(**params_dict)
    return fields


class MetaModel(type):
    def __new__(cls, name, bases, dct):
        if name != "Model":
            docstring = dct.get('__doc__', '')
            if docstring:
                fields = parse_docstring(docstring)
                for field_name, field in fields.items():
                    dct[field_name] = field
                dct["_fields"] = fields
        return super().__new__(cls, name, bases, dct)


class Model(metaclass=MetaModel):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def create_table(cls, params):
        columns = []
        pk = None
        for name, field in cls._fields.items():
            column_def = f"{name} {field.column_type}"
            if field.primary_key:
                column_def += " PRIMARY KEY"
                pk = name
            if field.foreign_key:
                column_def += f" REFERENCES {field.foreign_key.ref_table}"
            columns.append(column_def)

        create_table_command = f"CREATE TABLE IF NOT EXISTS {cls.__name__} ({', '.join(columns)})"
        try:
            with get_connection(params) as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_command)
                print(f"Table {cls.__name__} created successfully!")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error creating table {cls.__name__}: {error}")

    def save(self, params):
        columns = [name for name in self.__class__._fields]
        values = [getattr(self, name) for name in columns]
        insert_command = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(self.__class__.__name__),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        try:
            with get_connection(params) as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_command, values)
                    conn.commit()
                    print(f"Record inserted successfully into {self.__class__.__name__} table.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error inserting data into {self.__class__.__name__} table: {error}")


def get_connection(params):
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


class Patient(Model):
    """
    patient_id: IntegerField(primary_key=True)
    name: CharField(max_length=100)
    dob: CharField(max_length=10)
    gender: CharField(max_length=10)
    """

    @classmethod
    def generate_data(cls, n):
        first_names = ['John', 'Jane', 'Michael', 'Emma', 'William', 'Olivia']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
        genders = ['Male', 'Female']

        for _ in range(n):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            full_name = f"{first_name} {last_name}"
            dob = f"{random.randint(1950, 2010)}-{random.randint(1, 12):02}-{random.randint(1, 28):02}"
            gender = random.choice(genders)
            yield cls(name=full_name, dob=dob, gender=gender)


class Doctor(Model):
    """
    doctor_id: IntegerField(primary_key=True)
    name: CharField(max_length=100)
    specialty: CharField(max_length=100)
    """

    @classmethod
    def generate_data(cls, n):
        first_names = ['Dr. John', 'Dr. Jane', 'Dr. Michael', 'Dr. Emma', 'Dr. William', 'Dr. Olivia']
        specialties = ['Cardiology', 'Pediatrics', 'Orthopedics', 'Oncology', 'Neurology']

        for _ in range(n):
            doctor_name = random.choice(first_names)
            specialty = random.choice(specialties)
            yield cls(name=doctor_name, specialty=specialty)


class Appointment(Model):
    """
    appointment_id: IntegerField(primary_key=True)
    patient_id: IntegerField(foreign_key='Patient.patient_id')
    doctor_id: IntegerField(foreign_key='Doctor.doctor_id')
    appointment_date: CharField(max_length=10)
    appointment_time: CharField(max_length=5)
    """

    @classmethod
    def generate_data(cls, n, patient_ids, doctor_ids):
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 12, 31)

        for _ in range(n):
            patient_id = random.choice(patient_ids)
            doctor_id = random.choice(doctor_ids)
            appointment_date = f"{random_date(start_date, end_date)}"
            appointment_time = f"{random_time()}"
            yield cls(patient_id=patient_id, doctor_id=doctor_id, appointment_date=appointment_date,
                      appointment_time=appointment_time)


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)


def random_time():
    return datetime.time(random.randint(8, 17), random.randint(0, 59))

