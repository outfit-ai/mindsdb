def transform_query(sql_query):
    """
    Extracts database name from a SQL query and transforms it into a wrapped query format.

    Parameters:
    sql_query (str): The original SQL query

    Returns:
    tuple: (database_name, transformed_query)
    """
    # Clean the query by removing extra whitespace
    sql_query = ' '.join(sql_query.split())

    import re

    # Look for database.table pattern


    db_pattern = r"FROM ([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)"
    matches = re.findall(db_pattern, sql_query)

    if not matches:
        return sql_query

    database_name = matches[0][0]

    # Replace database reference with just schema.table
    transformed_query = re.sub(
        f"{database_name}\\.([a-zA-Z0-9_]+)",
        r"\1",
        sql_query
    )

    transformed_query = transformed_query.replace('"""', '')
    transformed_query = transformed_query.replace(';', '')
    # Wrap the query
    final_query = f'SELECT * FROM {database_name}({transformed_query});'

    return final_query

def tests():
    # Test with the example query
    example_query = """\"\"\"
    SELECT lineitem->>'item' AS product_id,
        SUM((lineitem->>'quantity')::int) AS total_quantity_sold
    FROM snk_psql_shopify.orders,
        LATERAL json_array_elements(orders.line_items::json) AS lineitem
    GROUP BY product_id
    ORDER BY total_quantity_sold DESC
    LIMIT 10
    \"\"\"
    """

    result = transform_query(example_query)

    print(result)

if __name__ == "__main__":
    tests()