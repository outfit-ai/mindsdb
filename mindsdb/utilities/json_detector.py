
import re

def needs_native_query(sql_query):
    if has_date_functions(sql_query) or has_json(sql_query) or has_inner_joins(sql_query):
        return True
    return False

def has_date_functions(sql_query):
    results = check_date_functions(sql_query)
    return results.get('has_date_functions', False)

def has_json(sql_query):
    results = analyze_json_operations(sql_query)
    return results.get('contains_json', False)

def has_inner_joins(sql_query):
    results = check_inner_joins(sql_query)
    return results.get('has_inner_joins', False)

def check_inner_joins(sql_query):
    """
    Check if a PostgreSQL query contains inner joins and analyze their structure.

    Args:
        sql_query (str): The SQL query to analyze

    Returns:
        dict: Dictionary containing:
            - has_inner_joins (bool): Whether inner joins were found
            - join_count (int): Number of inner joins found
            - join_details (list): Details of each join found
            - normalized_query (str): Query with standardized join syntax
    """

    # Normalize the query: remove extra whitespace and make case-insensitive
    normalized_query = ' '.join(sql_query.strip().split()).upper()

    # Patterns to match different inner join syntaxes
    join_patterns = [
        # Explicit INNER JOIN syntax
        (r'INNER\s+JOIN\s+(\w+)(?:\s+AS\s+(\w+))?\s+ON\s+([^;]+?)'
         r'(?:\s+(?:INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|$)'),

        # Just JOIN syntax (implicit inner join)
        (r'\sJOIN\s+(\w+)(?:\s+AS\s+(\w+))?\s+ON\s+([^;]+?)'
         r'(?:\s+(?:INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|$)'),

        # Old-style implicit inner join with comma
        (r'FROM\s+(\w+)(?:\s+AS\s+\w+)?\s*,\s*(\w+)(?:\s+AS\s+\w+)?\s+WHERE\s+([^;]+?)'
         r'(?:\s+GROUP|\s+ORDER|\s+LIMIT|$)')
    ]

    join_details = []
    total_joins = 0

    # Function to clean condition text
    def clean_condition(condition_text):
        # Remove trailing JOIN/WHERE/GROUP/ORDER/LIMIT clauses
        for clause in ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN',
                      'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT']:
            if f' {clause} ' in condition_text:
                condition_text = condition_text.split(f' {clause} ')[0]
        return condition_text.strip()

    for pattern in join_patterns:
        matches = re.finditer(pattern, normalized_query)
        for match in matches:
            total_joins += 1
            if match.group(0).startswith('FROM'):  # Old-style join
                join_details.append({
                    'type': 'implicit_comma',
                    'tables': [match.group(1), match.group(2)],
                    'condition': clean_condition(match.group(3)),
                    'position': match.start()
                })
            else:  # Modern join syntax
                # Check if it's an explicit INNER JOIN or implicit JOIN
                is_explicit = 'INNER JOIN' in match.group(0)
                join_details.append({
                    'type': 'explicit' if is_explicit else 'implicit',
                    'table': match.group(1),
                    'alias': match.group(2) if match.group(2) else None,
                    'condition': clean_condition(match.group(3)),
                    'position': match.start()
                })

    # Sort join details by position in query
    join_details.sort(key=lambda x: x['position'])

    return {
        'has_inner_joins': total_joins > 0,
        'join_count': total_joins,
        'join_details': join_details,
        'normalized_query': normalized_query
    }

def format_join_results(results):
    """
    Format the join analysis results in a readable way.

    Args:
        results (dict): Results from check_inner_joins

    Returns:
        str: Formatted string with results
    """
    if not results['has_inner_joins']:
        return "No inner joins found in the query."

    output = f"Found {results['join_count']} inner join(s) in the query:\n\n"

    for i, join in enumerate(results['join_details'], 1):
        output += f"Join #{i}:\n"
        output += f"Type: {join['type']}\n"

        if join['type'] == 'implicit_comma':
            output += f"Tables: {', '.join(join['tables'])}\n"
        else:
            output += f"Table: {join['table']}\n"
            if join['alias']:
                output += f"Alias: {join['alias']}\n"

        output += f"Condition: {join['condition']}\n"
        output += f"Position: {join['position']}\n\n"

    return output

# Example usage and testing
def test_inner_joins():
    # Test queries with different join syntaxes
    test_queries = [
        # Explicit INNER JOIN
        """
        SELECT * FROM table1
        INNER JOIN table2 ON table1.id = table2.id
        """,

        # Implicit JOIN
        """
        SELECT * FROM table1
        JOIN table2 AS t2 ON table1.id = t2.id
        """,

        # Old-style comma join
        """
        SELECT * FROM table1, table2
        WHERE table1.id = table2.id
        """,

        # Multiple joins
        """
        SELECT * FROM table1
        INNER JOIN table2 ON table1.id = table2.id
        JOIN table3 AS t3 ON table2.id = t3.id
        WHERE t3.value > 10
        """,

        # Query with no joins
        "SELECT * FROM table1 WHERE id > 10"
    ]

    for i, query in enumerate(test_queries, 1):
        print(f"\nTest Query #{i}:")
        print("-" * 50)
        print(f"Query: {query.strip()}")
        results = check_inner_joins(query)
        print("\nAnalysis:")
        print(format_join_results(results))

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
    test_inner_joins()