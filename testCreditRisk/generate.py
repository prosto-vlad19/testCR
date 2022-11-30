import argparse
import random
import re
import string
import time
from configparser import ConfigParser, Error
from datetime import datetime, timedelta

import pymysql as pymysql
from pymysql.constants import CLIENT
from pyspark.sql import SparkSession


class GeneratorList:
    spliter = "||"
    NUMBER_OF_YEARS = 5
    DAYS_IN_YEAR = 365
    NUMBER_OF_FILES = 1
    ONE_TENTH = []

    def __init__(self):
        pass

    def random_date_2(self):
        return (datetime.now() -
                timedelta(days=random.randint(0, self.NUMBER_OF_YEARS * self.DAYS_IN_YEAR))).date()

    def random_latin_string(self):
        text = [random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in
                range(10)]

        return ''.join(text)

    def random_russian_string(self):
        text = [random.choice([chr(1040 + i) for i in range(64)]) for _ in range(10)]
        return "".join(text)

    def random_even_number(self):
        number = random.randint(1, 100_000_001)
        if number % 2 == 1:
            number += 1
        return number

    def random_eight_numb(self):
        random_numbers = random.choice(range(1, 21))
        random_eight = [str(random.choice(range(10))) for _ in range(8)]
        return str(random_numbers) + "," + "".join(random_eight)

    def merge(self):
        stri = [str(self.random_date_2()), self.random_latin_string(), self.random_russian_string(), \
                str(self.random_even_number()), str(self.random_eight_numb())]
        return self.spliter.join(stri) + self.spliter

    def multi_merge(self, n_row=100_000):
        list_stri = []
        for i in range(n_row):
            stri = [str(self.random_date_2()), self.random_latin_string(), self.random_russian_string(), \
                    str(self.random_even_number()), str(self.random_eight_numb())]
            list_stri.append(self.spliter.join(stri) + self.spliter + "\n")
        return list_stri

    def generate_files(self, data=None, path=None, number_of_files=100):
        if path:
            with open(path, "w") as file:
                for row in data:
                    file.write(row)
            file.close()
        else:
            data = self.multi_merge()
            with open("./files/file_" + str(number_of_files) + ".csv", "w") as file:
                for row in data:
                    file.write(row)
            file.close()

    def csv_to_df_convertor(self, number_of_files):
        relative_path = "./files" + "/file_" + str(number_of_files) + ".csv"
        session = SparkSession.builder.appName("CSV to Dataset") \
            .master("local[*]").getOrCreate()
        df = session.read.options(delimiter="||").csv(path=relative_path)
        df.show(5)
        # session.stop()
        return df

    def csv_union(self, path, *args):
        with open(path, "w") as delete:
            delete.close()

        with open(path, "a") as fout:
            for i in range(len(args[0])):
                with open(args[0][i], "r", encoding="utf-8") as f:
                    fout.writelines(f)

    def delete(self, regexp, nums=100):
        row_number_start = 0
        row_number_end = 0
        for i in range(nums):
            with open(".//files/file_" + str(i + 1) + ".csv", "r+") as file:
                # with open("./test.csv","r+") as file:
                data = file.readlines()
                row_number_start += len(data)
                file.seek(0)
                for row in data:
                    if re.search(regexp, row) is None:
                        file.write(row)
                file.truncate()
            file.close()

        for i in range(nums):
            with open(".//files/file_" + str(i + 1) + ".csv", "r") as file:
                data = file.readlines()
                row_number_end += len(data)

        return row_number_start - row_number_end

    def get_config(self, config_path):
        file_config = config_path
        config = ConfigParser()
        config.read(file_config)
        config_db, config_path = [list() for _ in range(2)]
        for section in config.sections():
            for parameter_name in config[section]:
                parameter = config[section][parameter_name]
                if section == 'DB':
                    config_db.append(int(parameter) if parameter.isdigit() else parameter)
                elif section == 'PATH':
                    config_path.append(parameter)

        return config_db, config_path

    def create_connection_to_server_and_create_db(self, sql_script_path, host, user, passwd):
        try:
            connection = pymysql.connect(
                host=host,
                user=user,
                passwd=passwd,
                client_flag=CLIENT.MULTI_STATEMENTS
            )
            try:
                with connection.cursor() as cursor:
                    with open(sql_script_path) as file:
                        script = file.read()
                        cursor.execute(script)
                connection.commit()
            except Exception:
                pass
        except Error as e:
            pass

    def get_connection_to_db(self, host, user, passwd, db_name):
        connection = None
        try:
            connection = pymysql.connect(
                host=host,
                user=user,
                passwd=passwd,
                database=db_name,
                client_flag=CLIENT.MULTI_STATEMENTS
            )
        except Error as e:
            print(f"The error '{e}' occurred")

        return connection

    def import_to_db(self, file):
        spark = SparkSession.builder.appName("CSV to DB").master("local[*]"). \
            config("spark.jars", "/home/vlad/Загрузки/mysql-connector-java-8.0.29.jar").getOrCreate()

        df = spark.read.format("csv").option("delimiter", "||").load(file)
        df = df.drop("_c5")
        df = df.selectExpr("_c0 as date", "_c1 as lat_symb", "_c2 as ru_symb", \
                           "_c3 as int_num", "_c4 as float_num")
        config_db, config_path = self.get_config("./DB/config_server.ini")
        host = config_db[0]
        user = config_db[1]
        password = str(config_db[2])
        config_db[3]
        create_connection_to_server_and_create_db_path = config_path[0]
        self.create_connection_to_server_and_create_db(create_connection_to_server_and_create_db_path, host, user,
                                                       password)
        dbConnectionUrl = "jdbc:mysql://localhost:3306/TestDB"
        try:
            jdb_curl = dbConnectionUrl
            df.write.format("jdbc").option("url", jdb_curl) \
                .mode("overwrite") \
                .option("dbtable", "test1") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("user", "root") \
                .option("password", "password") \
                .save()
        except Exception as e:
            print(e)
        spark.stop()

    def execute_sql_file(self, connection, sql_script_path):
        try:
            with connection.cursor() as cursor:
                with open(sql_script_path) as file:
                    script = file.read()
                    cursor.execute(script)
                connection.commit()
        except Error as e:
            print(f"The error '{e}' occurred")
        return cursor

    def get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-generation_files', action='store_true',help="text file generation")
        parser.add_argument('-union_files', action='store_true',help="merge all files into one")
        parser.add_argument('-delete_row',help="deleting from all files a line with a given combination of characters")
        parser.add_argument('-import_file',help="file import procedure")
        parser.add_argument('-sum_and_median', action='store_true',help= "calculating the sum of all integers and the median of all fractional numbers")
        args = parser.parse_args()
        return {'generation_files': args.generation_files,
                'union_files': args.union_files,
                'delete_row': args.delete_row,
                'import_file': args.import_file,
                'sum_and_median': args.sum_and_median,
                }


if __name__ == "__main__":
    a = time.time_ns()
    gene = GeneratorList()
    args = gene.get_args()
    if args["generation_files"]:
        for i in range(100):
            gene.generate_files(number_of_files=i + 1)
            name = "df" + f"{i + 1}"
    if args["delete_row"]:
        print("deleted", gene.delete(args["delete_row"], 100), "row")
    if args["union_files"]:
        files = []
        for i in range(100):
            files.append(f".//files/file_{i + 1}.csv")
        gene.csv_union(".//files/combine_file.csv", files)
    if args["import_file"]:
        gene.import_to_db(args["import_file"])
    if args["sum_and_median"]:
        config_db, config_path = gene.get_config("./DB/config_server.ini")
        host = config_db[0]
        user = config_db[1]
        password = str(config_db[2])
        db_name = config_db[3]
        connection = gene.get_connection_to_db(host, user, password, db_name)
        result = gene.execute_sql_file(connection, "./DB/sum_and_avg.sql").fetchall()
        for row in result:
            print(row)

