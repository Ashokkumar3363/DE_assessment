import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, Date, Time, MetaData, Table

# MySQL database credentials
host = 'localhost'
username = 'root'
password = 'mysql'
database_name = 'Aidetic'


# Create a connection string
connection_str = f"mysql+pymysql://{username}:{password}@{host}/{database_name}"

# Create a SQLAlchemy engine
engine = create_engine(connection_str)

# Path to your CSV file
csv_file_path = '/home/ashok/Downloads/database.csv'

# Read CSV into Pandas DataFrame
df = pd.read_csv(csv_file_path)

# Table name in MySQL
table_name = 'neic_earthquakes'
metadata = MetaData()
# Create table based on DataFrame columns and types
earthquakes = Table('neic_earthquakes', metadata,
    Column('Date', Date),
    Column('Time', Time),
    Column('Latitude', Float),
    Column('Longitude', Float),
    Column('Type', String(255)),
    Column('Depth', Float),
    Column('Depth_Error', String(255)),
    Column('Depth_Seismic_Stations', Float),
    Column('Magnitude', Float),
    Column('Magnitude_Type', String(255)),
    Column('Magnitude_Error', String(255)),
    Column('Magnitude_Seismic_Stations', Float),
    Column('Azimuthal_Gap', Float),
    Column('Horizontal_Distance', Float),
    Column('Horizontal_Error', Float),
    Column('Root_Mean_Square', Float),
    Column('ID', String(255)),
    Column('Source', String(255)),
    Column('Location_Source', String(255)),
    Column('Magnitude_Source', String(255)),
    Column('Status', String(255))
)

metadata.create_all(engine)

# Insert data into MySQL table
df.to_sql(table_name, con=engine, if_exists="replace", index=False)

print(f"Data from {csv_file_path} inserted into {table_name} table in the {database_name} database.")
