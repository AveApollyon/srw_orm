import psycopg2
from time import time
import matplotlib.pyplot as plt
import random
import datetime
from psycopg2 import sql
from tqdm import tqdm

# Database parameters
db_params = {
    'dbname': 'your_db',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost'
}

# Define a function to get a database connection
def get_connection(params):
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def measure_generation_time(model, n_values):
    times = []
    for n in tqdm(n_values, desc=f'Generating {model.__name__} data'):
        start_time = time()
        for _ in range(n):
            model.generate_data(1)
        end_time = time()
        times.append(end_time - start_time)
    return times

def measure_query_time(query_func, n_values):
    times = []
    for n in tqdm(n_values, desc=f'Running {query_func.__name__} queries'):
        start_time = time()
        query_func(n)
        end_time = time()
        times.append(end_time - start_time)
    return times

def select_all_query(n):
    with get_connection(db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Patient LIMIT %s", (n,))
            cur.fetchall()

def select_where_query(n):
    with get_connection(db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Patient WHERE patient_id = %s", (random.randint(1, 10),))
            cur.fetchall()

def select_like_query(n):
    with get_connection(db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Patient WHERE name LIKE %s", ('%Smith%',))
            cur.fetchall()

def insert_query(n):
    with get_connection(db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO Patient (name, dob, gender) VALUES %s", [(f"Patient-{i}", f"2000-01-01", "Male") for i in range(n)])
            conn.commit()

def delete_query(n):
    with get_connection(db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM Patient WHERE patient_id IN %s", (tuple(range(1, n+1)),))
            conn.commit()

if __name__ == "__main__":
    from lib.orm import Patient, Doctor, Appointment, MetaModel, IntegerField, StringField, ForeignKey, parse_docstring

    # Create tables if not exists
    Patient.create_table(db_params)
    Doctor.create_table(db_params)
    Appointment.create_table(db_params)

    # Define the number of rows to generate and query
    n_values = [10, 50, 100, 500, 1000]

    # Measure generation times
    patient_generation_times = measure_generation_time(Patient, n_values)
    doctor_generation_times = measure_generation_time(Doctor, n_values)
    appointment_generation_times = measure_generation_time(Appointment, n_values)

    # Measure query times
    select_all_times = measure_query_time(select_all_query, n_values)
    select_where_times = measure_query_time(select_where_query, n_values)
    select_like_times = measure_query_time(select_like_query, n_values)
    insert_times = measure_query_time(insert_query, n_values)
    delete_times = measure_query_time(delete_query, n_values)

    # Plot generation time graphs
    plt.figure(figsize=(12, 6))
    plt.plot(n_values, patient_generation_times, marker='o', label='Patient')
    plt.plot(n_values, doctor_generation_times, marker='o', label='Doctor')
    plt.plot(n_values, appointment_generation_times, marker='o', label='Appointment')
    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.title('Generation Time for ORM Models')
    plt.xticks(n_values)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('generation_times.png')
    plt.show()

    # Plot query time graphs
    plt.figure(figsize=(12, 12))

    plt.subplot(3, 2, 1)
    plt.plot(n_values, select_all_times, marker='o')
    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.title('SELECT * Query Time')
    plt.grid(True)

    plt.subplot(3, 2, 2)
    plt.plot(n_values, select_where_times, marker='o')
    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.title('SELECT WHERE Query Time')
    plt.grid(True)

    plt.subplot(3, 2, 3)
    plt.plot(n_values, select_like_times, marker='o')
    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.title('SELECT LIKE Query Time')
    plt.grid(True)

    plt.subplot(3, 2, 4)
    plt.plot(n_values, insert_times, marker='o')
    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.title('INSERT Query Time')
    plt.grid(True)

    plt.subplot(3, 2, 5)
    plt.plot(n_values, delete_times, marker='o')
    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.title('DELETE Query Time')
    plt.grid(True)

    plt.tight_layout()
    plt.savefig('query_times.png')
    plt.show()
