import pandas as pd


class UserDataWrangler:
    """This class holds methods to transform data"""
    def __init__(self, path: str) -> None:
        self.df = pd.read_csv(path)
        self.mapdict = {}  # Contains all created mappers -> col name: {mapper}

    def drop_missing_values(self, columns_to_change: list) -> None:
        """Takes a list of columns as an argument
        and drops missing values in them"""
        self.df.dropna(subset=columns_to_change, how='any', inplace=True)
        self.df.reset_index(inplace=True)

    def split_column(self, col: str, new_column: str, index: int) -> None:
        """Creates new column with the value of provided index
        by splitting the original column"""
        self.df[new_column] = self.df[col].apply(lambda x: x.split()[index])

    def change_format_to_seconds(self, col: str) -> None:
        """Changes the chosen column's format to timestamp(seconds)"""
        self.df[col] = pd.to_datetime(self.df[col], infer_datetime_format=True)
        self.df[col] = (self.df[col] - pd.Timestamp('1970-01-01')) \
            // pd.Timedelta('1s')

    def __create_mapper(self, col_to_change: str) -> dict:
        """Creates a mapper for values in column. Starts from 1"""
        unique_values = self.df[col_to_change].unique()
        self.mapper = {}
        for idx, value in enumerate(unique_values):
            self.mapper[value] = idx + 1
        self.mapdict[col_to_change] = self.mapper
        return self.mapper

    def apply_mapper(self, col_to_change: str) -> None:
        """Uses _create_mapper method to change
        the column values to be numeric"""
        self.df[col_to_change] = self.df[col_to_change].map(
            self.__create_mapper(col_to_change))

    def is_greater(self, col: str, new_col: str,
                   value_to_compare: int) -> None:
        """Creates a column which shows
        whether column value is greater than value_to_compare"""
        self.df[new_col] = self.df[col].apply(
            lambda x: True if x > value_to_compare else False)

    def save_to_db(self, columns_to_save: list,
                   engine: object, table: str) -> None:
        """Saves sorted by id dataframe to Database table"""
        df_to_save = self.df[columns_to_save].sort_values(by=["id"])
        df_to_save.to_sql(table, engine, if_exists='append', index=False)
        print("Successfully loaded to db")
