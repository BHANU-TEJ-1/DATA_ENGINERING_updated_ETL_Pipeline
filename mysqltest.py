import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://root:#*mysql*#@localhost:3306/olist_db"
)

df = pd.read_csv("C:/Users/TEJ/Downloads/archive/olist_order_reviews_dataset.csv")

df.to_sql(
    "order_reviews",
    con=engine,
    if_exists="replace",
    index=False
)