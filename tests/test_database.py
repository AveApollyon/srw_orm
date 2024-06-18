# tests/test_database.py
import unittest
from lib.database import connect, create_tables

class TestDatabaseFunctions(unittest.TestCase):

    def setUp(self):
        # Подключаемся к тестовой базе данных (можно использовать отдельную тестовую БД)
        self.conn = connect()
        self.cur = self.conn.cursor()

    def test_create_tables(self):
        # Тест на создание таблиц
        create_tables()
        # Проверяем, что таблицы существуют
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = [table[0] for table in self.cur.fetchall()]
        self.assertIn('patients', tables)
        self.assertIn('doctors', tables)
        self.assertIn('appointments', tables)

    def tearDown(self):
        # Закрываем соединение с базой данных
        self.cur.close()
        self.conn.close()

if __name__ == '__main__':
    unittest.main()
