from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://postgres:123456@localhost:5432/warehouse_db"

try:
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        print("✅ Connected Successfully!")

except Exception as e:
    print(e)