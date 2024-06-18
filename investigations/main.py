import random
import timeit
import matplotlib.pyplot as plt
from lib.orm import DataBase, Patient, Doctor, Appointment, DatabaseSandbox
from faker import Faker

fake = Faker('ru_RU')


def measure_query_time(db, query, params=None, number=1):
    """
    Измеряет время выполнения SQL-запроса к базе данных.

    Args:
        db (DataBase): Объект для выполнения запросов к базе данных.
        query (str): Текст SQL-запроса.
        params (tuple or list, optional): Параметры запроса. Default is None.
        number (int, optional): Количество повторений запроса для измерения времени. Default is 1.

    Returns:
        list: Время выполнения запроса в секундах для каждого повторения.
    """
    if isinstance(params, list):
        stmt = lambda: [db.execute_query(query, param) for param in params]
    else:
        stmt = lambda: db.execute_query(query, params)

    times = timeit.repeat(stmt, repeat=number, number=1)
    return times


def plot_graph(x, y, title, xlabel, ylabel, filename):
    """
    Строит график по данным x и y и сохраняет его в файл.

    Args:
        x (list): Данные для оси X (например, количество строк).
        y (list of tuples): Данные для оси Y в формате [(y_values, label)].
        title (str): Заголовок графика.
        xlabel (str): Название оси X.
        ylabel (str): Название оси Y.
        filename (str): Имя файла для сохранения графика.
    """
    plt.figure()
    for i, (y_values, label) in enumerate(y):
        plt.plot(x, y_values, label=label, marker='o' if len(x) < 10 else '',
                 linestyle='-' if i % 3 == 0 else '--' if i % 3 == 1 else ':')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.savefig(filename)
    plt.show()


def generate_data_and_measure_time(db, model_class, n, repeat=1):
    """
    Генерирует данные для модели и измеряет среднее время их создания.

    Args:
        db (DataBase): Объект для выполнения запросов к базе данных.
        model_class (class): Класс модели (Patient, Doctor или Appointment).
        n (int): Количество объектов для генерации.
        repeat (int, optional): Количество повторений измерения времени. Default is 1.

    Returns:
        list: Среднее время генерации данных в секундах для каждого повторения.
    """
    def generate_data():
        if model_class == Patient:
            objects = db.generate_objects(n, model_class.generate_patients)
        elif model_class == Doctor:
            objects = db.generate_objects(n, model_class.generate_doctors)
        elif model_class == Appointment:
            patients = db.generate_objects(n, Patient.generate_patients)
            doctors = db.generate_objects(n, Doctor.generate_doctors)
            objects = model_class.generate_appointments(db, n)
        else:
            raise ValueError("Unsupported model_class")

    times = timeit.repeat(generate_data, number=1, repeat=repeat)

    return times


def setup_database(db):
    """
    Создает таблицы в базе данных для моделей Patient, Doctor и Appointment.

    Args:
        db (DataBase): Объект для выполнения запросов к базе данных.
    """
    db.create_table(Patient)
    db.create_table(Doctor)
    db.create_table(Appointment)


def execute_queries(queries, sizes, db):
    """
    Выполняет набор SQL-запросов и строит графики времени их выполнения.

    Args:
        queries (list of tuples): Список кортежей (query, param_func).
            query (str): Текст SQL-запроса.
            param_func (function or tuple): Функция или параметры для запроса.
        sizes (list): Список размеров таблиц для выполнения запросов.
        db (DataBase): Объект для выполнения запросов к базе данных.
    """
    for query, param_func in queries:
        query_times = []
        for size in sizes:
            new_patients = db.generate_objects(size, Patient.generate_patients)
            db.replace_all_data(Patient, new_patients)

            new_doctors = db.generate_objects(size, Doctor.generate_doctors)
            db.replace_all_data(Doctor, new_doctors)

            new_appointments = Appointment.generate_appointments(db, size)
            db.replace_all_data(Appointment, new_appointments)

            if "INSERT" in query:
                params = param_func(size)
                time_taken = measure_query_time(db, query, params=params, number=1)
            else:
                time_taken = measure_query_time(db, query,
                                                params=param_func() if callable(param_func) else param_func)
            query_times.append(time_taken)

        plot_graph(sizes, [(query_times, f'Query: {query[:30]}')],
                   f'Время выполнения запроса: {query[:30]}...',
                   'Количество строк', 'Время (с)',
                   f'img/query_time_{query[:30].replace(" ", "_").replace("*", "all")}.png')


if __name__ == "__main__":
    source_db_params = {
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

    with DatabaseSandbox(source_db_params, sandbox_db_params) as sandbox:
        db = DataBase(**sandbox_db_params)

        setup_database(db)

        sizes = [100, 1000]

        times_patient = [generate_data_and_measure_time(db, Patient, size) for size in sizes]
        times_doctor = [generate_data_and_measure_time(db, Doctor, size) for size in sizes]
        times_appointment = [generate_data_and_measure_time(db, Appointment, size) for size in sizes]

        plot_graph(sizes, [(times_patient, 'Patients'), (times_doctor, 'Doctors'), (times_appointment, 'Appointments')],
                   'Время генерации данных',
                   'Количество строк', 'Время (с)', 'generation_time.png')

        queries = [
            ("SELECT * FROM patient", None),
            ("SELECT * FROM doctor", None),
            ("SELECT * FROM appointment", None),
            ("SELECT * FROM patient WHERE age > 50", None),
            ("SELECT * FROM doctor WHERE specialty = %s", ('Cardiologist',)),
            ("SELECT * FROM appointment WHERE patient_id = %s", lambda: (random.randint(1, sizes[-1]),)),

            ("INSERT INTO patient (name, age, gender) VALUES (%s, %s, %s)",
             lambda size: [(fake.name(), random.randint(18, 80), random.choice(['Male', 'Female'])) for _ in range(size)]),
            ("INSERT INTO doctor (name, specialty) VALUES (%s, %s)",
             lambda size: [(fake.name(), 'Cardiologist') for _ in range(size)]),
            ("INSERT INTO appointment (patient_id, doctor_id, appointment_date) VALUES (%s, %s, %s)",
             lambda size: [(random.randint(1, size), random.randint(1, size), fake.date_time_between(start_date='-2y', end_date='-1y')) for _ in range(size)]),

            ("DELETE FROM patient WHERE age < 25", None),
            ("DELETE FROM doctor WHERE specialty = 'Dermatologist'", None),
            ("DELETE FROM appointment WHERE appointment_date < %s", (fake.date_time_between(start_date='-2y', end_date='-1y'),)),

            ("SELECT COUNT(*) FROM patient", None),
            ("SELECT COUNT(*) FROM doctor", None),
            ("SELECT COUNT(*) FROM appointment", None)
        ]

        execute_queries(queries, sizes, db)
