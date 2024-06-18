"""
Модуль тестирования ORM с использованием фреймворка unittest.

Этот модуль тестирует функциональность ORM, включая создание таблиц,
вставку данных, удаление, подсчет записей и операции резервного копирования и восстановления.
"""

import unittest
from lib.orm import DataBase, Patient, Doctor, Appointment, DatabaseSandbox
from datetime import datetime
import psycopg2

class TestDataBase(unittest.TestCase):
    """
    Класс для тестирования функциональности ORM.
    """

    @classmethod
    def setUpClass(cls):
        """
        Метод, выполняемый один раз перед запуском всех тестов.
        Настраивает параметры подключения к основной и песочнице базам данных.
        """
        cls.db_params = {
            'dbname': 'science_work_lab',
            'user': 'postgres',
            'password': '52436150',
            'host': 'localhost'
        }

        cls.sandbox_params = {
            'dbname': 'science_work_lab_sandbox',
            'user': 'postgres',
            'password': '52436150',
            'host': 'localhost'
        }

    def setUp(self):
        """
        Метод, выполняемый перед каждым тестом.
        Создает песочницу и инициализирует объект базы данных.
        """
        self.sandbox = DatabaseSandbox(self.db_params, self.sandbox_params)
        self.sandbox.__enter__()
        self.db = DataBase(**self.sandbox_params)

    def tearDown(self):
        """
        Метод, выполняемый после каждого теста.
        Уничтожает песочницу.
        """
        self.sandbox.__exit__(None, None, None)

    def test_create_and_drop_tables(self):
        """
        Тестирование создания и удаления таблиц.
        Проверяет, что таблицы правильно создаются и удаляются.
        """
        self.db.create_table(Patient)
        self.db.create_table(Doctor)
        self.db.create_table(Appointment)

        result = self.db.execute_query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        table_names = [row[0] for row in result]

        self.assertIn('patient', table_names)
        self.assertIn('doctor', table_names)
        self.assertIn('appointment', table_names)

        self.db.drop_table(Patient)
        self.db.drop_table(Doctor)
        self.db.drop_table(Appointment)

        result = self.db.execute_query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        table_names = [row[0] for row in result]

        self.assertNotIn('patient', table_names)
        self.assertNotIn('doctor', table_names)
        self.assertNotIn('appointment', table_names)

    def test_insert_and_query_data(self):
        """
        Тестирование вставки и запроса данных.
        Проверяет, что данные правильно вставляются и запрашиваются.
        """
        self.db.drop_table(Patient)
        self.db.create_table(Patient)
        patients = Patient.generate_patients(5)
        self.db.save_objects(patients)

        result = self.db.execute_query("SELECT * FROM patient")
        self.assertEqual(len(result), 5)

    def test_delete_all_data(self):
        """
        Тестирование удаления всех данных из таблицы.
        Проверяет, что данные правильно удаляются.
        """
        self.db.drop_table(Patient)
        self.db.create_table(Patient)
        patients = Patient.generate_patients(5)
        self.db.save_objects(patients)

        self.db.delete_all_data(Patient)
        result = self.db.execute_query("SELECT * FROM patient")
        self.assertEqual(len(result), 0)

    def test_count_entries(self):
        """
        Тестирование подсчета записей в таблице.
        Проверяет, что количество записей правильно подсчитывается.
        """
        self.db.drop_table(Patient)
        self.db.create_table(Patient)
        patients = Patient.generate_patients(5)
        self.db.save_objects(patients)

        result = self.db.execute_query("SELECT COUNT(*) FROM patient")
        count = result[0][0]
        self.assertEqual(count, 5)

    def test_backup_and_restore_table(self):
        """
        Тестирование резервного копирования и восстановления таблицы.
        Проверяет, что данные правильно сохраняются и восстанавливаются.
        """
        self.db.drop_table(Patient)

        self.db.create_table(Patient)
        patients = Patient.generate_patients(5)
        self.db.save_objects(patients)

        backup_path = 'patient_backup.csv'
        self.db.backup_table(Patient, backup_path)

        self.db.delete_all_data(Patient)
        result = self.db.execute_query("SELECT * FROM patient")
        self.assertEqual(len(result), 0)

        self.db.restore_table(Patient, backup_path)
        result = self.db.execute_query("SELECT * FROM patient")
        self.assertEqual(len(result), 5)

if __name__ == '__main__':
    unittest.main()
