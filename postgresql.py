import psycopg2
import config
# TODO:
# добавить пустые колонки для логина и пароля

class Database:

    def __init__(self):
        self._database = config.PG_DATABASE
        self._user = config.PG_USER
        self._conn = None
        self._cur = None

    def connect(self):
        # Connect to an existing database
        self._conn = psycopg2.connect(f"dbname={self._database} user={self._user}")

        # Open a cursor to perform database operations
        self._cur = self._conn.cursor()

        # Создать таблицы
        self._create_table_units()
        self._create_table_clients()

    def _create_table_units(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS units (
                country_code VARCHAR(2),
                unit_id INTEGER,
                uuid VARCHAR(32),
                unit_name VARCHAR(30),
                PRIMARY KEY (country_code, unit_id)
            );
        """)

    def _create_table_clients(self):
        # first_order_type: 0 - Доставка, 1 - Самовывоз, 2 - Ресторан, 3 - Прочее
        self.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                country_code VARCHAR(2),
                unit_id INTEGER,
                phone VARCHAR(20),
                first_order_datetime TIMESTAMP,
                first_order_city VARCHAR(30),
                last_order_datetime TIMESTAMP,
                last_order_city VARCHAR(30),
                first_order_type INTEGER,
                sms_text VARCHAR(150),
                sms_text_city VARCHAR(30),
                ftp_path_city VARCHAR(15),
                PRIMARY KEY (country_code, unit_id, phone)
            );
        """)

    def execute(self, *args, **kwargs):
        # Execute a command: this creates a new table
        self._cur.execute(*args, **kwargs)

    def fetch(self, one=False):
        if not one:
            return self._cur.fetchall()
        if one:
            return self._cur.fetchone()

    def close(self):

        # Make the changes to the database persistent
        self._conn.commit()

        # Close communication with the database
        self._cur.close()
        self._conn.close()
