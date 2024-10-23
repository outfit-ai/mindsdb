from typing import Type
import re

from langchain.tools import BaseTool
from mindsdb_sql import parse_sql
from pydantic import BaseModel, Field
from mindsdb.utilities.native_query_transformer import transform_query
from mindsdb.utilities.json_detector import has_json

class _MindsDBSQLParserToolInput(BaseModel):
    tool_input: str = Field("", description="A SQL query to validate.")


class MindsDBSQLParserTool(BaseTool):
    name: str = "mindsdb_sql_parser_tool"
    description: str = "Parse a SQL query to check it is valid MindsDB SQL."
    args_schema: Type[BaseModel] = _MindsDBSQLParserToolInput

    def _clean_query(self, query: str) -> str:
        # Sometimes LLM can input markdown into query tools.
        cmd = re.sub(r'```(sql)?', '', query)
        return cmd

    def _query_options(self, query):
        yield query
        if '\\_' in query:
            yield query.replace('\\_', '_')

    def _run(self, query: str):
        """Validate the SQL query."""
        clean_query = self._clean_query(query)
        if has_json(clean_query):
            clean_query = transform_query(clean_query)
        for query in self._query_options(clean_query):
            try:
                ast_query = parse_sql(query, dialect='mindsdb')
                return "".join(f"valid query: {ast_query.to_string()}")
            except Exception as e:
                error = "".join(f"invalid query, with error: {e}")
                continue
        return error

def test():

    mindsdb_sql_parser_tool = MindsDBSQLParserTool(
        name=f'mindsdb_sql_parser_tool_yc',
        description="Parse a SQL query to check it is valid MindsDB SQL."
    )
    q = """\"\"\"
WITH extracted_titles AS (
    SELECT
        jsonb_array_elements(line_items) AS line_item
    FROM
        snk_psql_shopify.orders
)
SELECT
    line_item->>'title' AS product_title,
    COUNT(*) AS sales_count
FROM
    extracted_titles
GROUP BY
    product_title
ORDER BY
    sales_count DESC
LIMIT 10;
\"\"\"
"""

    q = """\"\"\"select * from db(select line_item->>'title' AS product_title from orders where order_id = 1);\"\"\"
        """
    output = mindsdb_sql_parser_tool._run(q)
    print(output)


if __name__ == "__main__":
    test()