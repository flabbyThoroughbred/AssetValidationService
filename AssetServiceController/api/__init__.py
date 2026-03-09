from .DbManager import DBManager

# Initialize tables if they don't exist
db = DBManager()
db.create_tables()