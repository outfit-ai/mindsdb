def has_json(sql_query):
    results = analyze_json_operations(sql_query)
    return results.get('contains_json', False)

def analyze_json_operations(sql_query):
    """
    Analyzes a PostgreSQL query for JSON operations and returns details about the JSON handling found.

    Parameters:
    sql_query (str): The SQL query to analyze

    Returns:
    dict: Analysis results containing JSON operations found
    """
    # Common JSON operators and functions in PostgreSQL
    json_operators = {
        '->>': 'JSON text extraction operator',
        '->': 'JSON object/array extraction operator',
        '#>': 'JSON path extraction operator',
        '#>>': 'JSON path text extraction operator',
        '@>': 'JSON containment operator',
        '<@': 'JSON contained by operator',
        '?': 'JSON key exists operator',
        '?|': 'JSON any key exists operator',
        '?&': 'JSON all keys exist operator'
    }

    json_functions = [
        'json_array_elements',
        'jsonb_array_elements',
        'json_array_elements_text',
        'json_extract_path',
        'json_object',
        'json_build_object',
        'json_build_array',
        'json_populate_record',
        'json_typeof',
        'json_strip_nulls',
        'jsonb_set',
        'jsonb_insert'
    ]

    # Initialize results
    results = {
        'contains_json': False,
        'operators_found': [],
        'functions_found': [],
        'type_casts': [],
        'details': []
    }

    # Check for JSON operators
    for operator, description in json_operators.items():
        if operator in sql_query:
            results['contains_json'] = True
            results['operators_found'].append({
                'operator': operator,
                'description': description
            })

    # Check for JSON functions
    for func in json_functions:
        if func.lower() in sql_query.lower():
            results['contains_json'] = True
            results['functions_found'].append(func)

    # Check for JSON/JSONB type casts
    if '::json' in sql_query:
        results['contains_json'] = True
        results['type_casts'].append('::json')
    if '::jsonb' in sql_query:
        results['contains_json'] = True
        results['type_casts'].append('::jsonb')

    # Add detailed analysis if JSON operations were found
    if results['contains_json']:
        details = []
        if results['operators_found']:
            details.append(f"Found JSON operators: {', '.join(op['operator'] for op in results['operators_found'])}")
        if results['functions_found']:
            details.append(f"Found JSON functions: {', '.join(results['functions_found'])}")
        if results['type_casts']:
            details.append(f"Found JSON type casts: {', '.join(results['type_casts'])}")
        results['details'] = details

    return results


def tests():
    # Example usage with the provided query
    example_query = """
    SELECT lineitem->>'item' AS product_id,
        SUM((lineitem->>'quantity')::int) AS total_quantity_sold
    FROM snk_psql_shopify.orders,
        LATERAL json_array_elements(orders.line_items::json) AS lineitem
    GROUP BY product_id
    ORDER BY total_quantity_sold DESC
    LIMIT 10
    """

    analysis = analyze_json_operations(example_query)

    # Output the analysis results


    print(analysis)


if __name__ == "__main__":
    tests()