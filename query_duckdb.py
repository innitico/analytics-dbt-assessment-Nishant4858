import pandas as pd 
import duckdb

def query_duckdb():
    connetion = duckdb.connect("dev.duckdb")
    provider_address_agg = connetion.execute("select * from provider_address_agg").fetchdf()
    print("This is the data in provider_address_agg table --- ")
    print(provider_address_agg)


if __name__ == "__main__":
    query_duckdb()
