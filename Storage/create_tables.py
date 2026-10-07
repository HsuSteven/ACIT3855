from models import Base, ENGINE

def create_tables():
    Base.metadata.create_all(ENGINE)
    print("Tables created successfully.")

if __name__ == "__main__":
    create_tables()