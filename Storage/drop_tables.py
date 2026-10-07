from models import Base, ENGINE

def drop_tables():
    Base.metadata.drop_all(ENGINE)
    print("Tables dropped successfully.")

if __name__ == "__main__":
    drop_tables()