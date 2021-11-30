import sqlalchemy


class DBConnector:
    """Accepts Postgres database credentials and creates engine"""
    def __init__(self, user: str, password: str,
                 host: str, database: str) -> None:
        self.engine = self.__connect_to_db(user, password, host, database)

    def __connect_to_db(self, user: str, password: str,
                        host: str, database: str) -> object:
        """Creates connection to db"""
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
        return engine

    def execute_query(self, query: str) -> None:
        """Executes given query"""
        with self.engine.connect() as connection:
            connection.execute(query)
