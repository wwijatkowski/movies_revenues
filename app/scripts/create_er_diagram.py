from pyspark.sql import SparkSession
from graphviz import Digraph
from app.config.config import DIM_DATE_LOCATION, DIM_MOVE_LOCATION, FACT_DAILY_REVENUE_LOCATION

def read_parquet_schema(file_path):
    spark = SparkSession.builder.appName("ParquetSchemaReader").getOrCreate()
    df = spark.read.parquet(file_path)
    schema = df.schema
    spark.stop()
    return schema

def schema_to_graph(schema, table_name, dot):
    with dot.subgraph(name='cluster_' + table_name) as c:
        c.attr(label=table_name)
        for field in schema.fields:
            c.node(field.name, label=f"{field.name} ({field.dataType.simpleString()})")

def main():
    
    # Read the schemas
    dim_movie_schema = read_parquet_schema(DIM_MOVE_LOCATION)
    fact_daily_revenue_schema = read_parquet_schema(FACT_DAILY_REVENUE_LOCATION)
    dim_date_schema = read_parquet_schema(DIM_DATE_LOCATION)

    # Create a Graphviz Digraph
    dot = Digraph(comment='ER Diagram')

    # Add tables to the graph
    schema_to_graph(dim_movie_schema, "dim_movie", dot)
    schema_to_graph(fact_daily_revenue_schema, "fact_daily_revenue", dot)
    schema_to_graph(dim_date_schema, "dim_date", dot)

    # Define relationships
    dot.edge('dim_movie.id', 'fact_daily_revenue.id', constraint='true')
    dot.edge('dim_date.date', 'fact_daily_revenue.date', constraint='true')

    # Save the diagram to a file
    dot.render('er_diagram', format='png')

if __name__ == "__main__":
    main()