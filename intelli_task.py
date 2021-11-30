from data_wrangler import UserDataWrangler
from db_connector import DBConnector


class IntelliTask(UserDataWrangler):

    def do_task(self) -> None:
        """Performs data transformation according to Task"""
        self.drop_missing_values(['full_name', 'email', 'language'])
        self.split_column('full_name', 'first_name', 0)
        self.split_column('full_name', 'last_name', -1)
        self.apply_mapper('language')
        self.apply_mapper('gender')
        self.change_format_to_seconds('last_login_at')
        self.change_format_to_seconds('created_at')
        self.change_format_to_seconds('date_of_birth')
        self.is_greater('profile_percentage', 'is_full_profile', 90)
        self.__import_to_db()

    def __import_to_db(self) -> None:
        """Creates users table, connection to postgres db
         and saves dataframe to db table"""
        db_connector = DBConnector('postgres', 'postgres',
                                   'localhost', 'intelliboard_task')
        query = """
            DROP TABLE IF EXISTS users;
            CREATE TABLE users(
                id BIGINT NOT NULL,
                first_name VARCHAR(35),
                last_name VARCHAR(35),
                gender SMALLINT,
                language SMALLINT,
                email VARCHAR(254),
                profile_percentage INTEGER,
                last_login_at BIGINT,
                date_of_birth BIGINT,
                created_at BIGINT,
                is_full_profile BOOLEAN
                );
                """
        db_connector.execute_query(query)
        columns_to_save = ['id', 'first_name', 'last_name',
                           'gender', 'language', 'email',
                           'profile_percentage', 'last_login_at',
                           'date_of_birth', 'created_at', 'is_full_profile']
        self.save_to_db(columns_to_save, db_connector.engine, 'users')


users_data = IntelliTask('intelli_test_task.csv')
users_data.do_task()
