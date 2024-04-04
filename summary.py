import sqlalchemy as sa
from sqlalchemy import func

# Database connection details (replace with your own)
db_user = "postgres"
db_host = "localhost"
db_port = "5432"  # Default PostgreSQL port
db_name = "air_quality"
table_name = "Civic_pm25"
column_name = "value"

# Construct database connection string
engine_string = f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"

# Create the SQLAlchemy engine
engine = sa.create_engine(engine_string)

# Create a session
with engine.connect() as connection:
    # Query for summary statistics
    result = connection.execute(
        sa.select([
            func.max(sa.column(column_name)).label("maximum"),
            func.min(sa.column(column_name)).label("minimum"),
            func.avg(sa.column(column_name)).label("mean"),
            func.percentile_cont(0.5).within_group(sa.column(column_name)).label("median")
        ]).from_(sa.table(table_name))
    )

    # Print the results
    row = result.fetchone()
    print("Summary Statistics:")
    print(f"Maximum: {row['maximum']}")
    print(f"Minimum: {row['minimum']}")
    print(f"Mean: {row['mean']}")
    print(f"Median: {row['median']}")