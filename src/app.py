import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
def connect():
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string, echo=True)
    return engine

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
engine = connect()

#2.1 Previamente limpiamos las tablas que existan
with open('./src/sql/drop.sql', 'r') as file:
    drop_tables_sql = file.read()
    engine.execute(drop_tables_sql)

#2.2 Procedemos a crear las tablas desde 0
with open('./src/sql/create.sql', 'r') as file:
    create_tables_sql = file.read()
    engine.execute(create_tables_sql)

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
try:
    with open('./src/sql/insert.sql', 'r') as file:
        insert_data_sql = file.read()
        engine.execute(insert_data_sql)
except Exception as e:
    print(f"Error al insertar datos: {e}")

# 4) Use pandas to print one of the tables as dataframes using read_sql function
df = pd.read_sql("SELECT * FROM publishers", engine)
print(df)