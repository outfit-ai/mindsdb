
import re

def needs_native_query(sql_query):
    if has_date_functions(sql_query) or has_json(sql_query):
        return True
    return False

def has_date_functions(sql_query):
    results = check_date_functions(sql_query)
    return results.get('has_date_functions', False)

def has_json(sql_query):
    results = analyze_json_operations(sql_query)
    return results.get('contains_json', False)


def check_date_functions(sql_query):
    """
    Check if a PostgreSQL query contains date/time functions.

    Args:
        sql_query (str): The SQL query to analyze

    Returns:
        dict: Dictionary containing:
            - has_date_functions (bool): Whether date functions were found
            - found_functions (list): List of date functions found
            - positions (list): List of tuples with (function, position) where found
    """

    # List of PostgreSQL date/time functions
    date_functions = [
        # Current date/time
        'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'LOCALTIME', 'LOCALTIMESTAMP', 'NOW',
        # Date/Time constructors
        'MAKE_DATE', 'MAKE_TIME', 'MAKE_TIMESTAMP', 'MAKE_TIMESTAMPTZ',
        # Extracting parts
        'DATE_PART', 'DATE_TRUNC', 'EXTRACT', 'CENTURY', 'DECADE', 'EPOCH', 'YEAR', 'MONTH', 'DAY',
        'HOUR', 'MINUTE', 'SECOND', 'MILLISECOND', 'MICROSECOND',
        # Formatting
        'TO_CHAR', 'TO_DATE', 'TO_TIMESTAMP',
        # Calculations
        'AGE', 'DATE_BIN', 'JUSTIFY_DAYS', 'JUSTIFY_HOURS', 'JUSTIFY_INTERVAL',
        # Timezone conversions
        'TIMEZONE', 'AT TIME ZONE',
        # Intervals
        'INTERVAL'
    ]

    # Create case-insensitive pattern
    pattern = r'\b(' + '|'.join(date_functions) + r')\b'

    # Find all matches
    matches = []
    for match in re.finditer(pattern, sql_query, re.IGNORECASE):
        matches.append((match.group(), match.start()))

    found_functions = [match[0] for match in matches]

    return {
        'has_date_functions': len(matches) > 0,
        'found_functions': list(set(found_functions)),  # Unique functions
        'positions': matches
    }

def format_results(results):
    """
    Format the results in a readable way.

    Args:
        results (dict): Results from check_date_functions

    Returns:
        str: Formatted string with results
    """
    if not results['has_date_functions']:
        return "No date functions found in the query."

    output = "Date functions found in the query:\n"
    for func in results['found_functions']:
        output += f"- {func}\n"

    output += "\nPositions in query:\n"
    for func, pos in results['positions']:
        output += f"- {func} at position {pos}\n"

    return output

# Example usage
def test_date_functions():
    # Test queries
    test_queries = [
        "SELECT * FROM users WHERE created_at > CURRENT_DATE - INTERVAL '1 day'",
        "SELECT id, name, DATE_TRUNC('month', created_at) FROM orders",
        "SELECT * FROM products"  # Query with no date functions
    ]

    for query in test_queries:
        print("\nAnalyzing query:", query)
        results = check_date_functions(query)
        print(format_results(results))

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
    test_date_functions()