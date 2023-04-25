import re
import sql_metadata


class DaskQueryParser:

    def __init__(self):
        self.query = None
        self.main_table = None
        self.cte = None
        self.base = None
        self.iterative = None
        self.final = None
        self.is_iter = False
        self.cte_params = []

    def parse(self, query):
        self.query = query
        tokens = sql_metadata.get_query_tokens(self.query)
        if len(tokens) > 0 and "WITH" == tokens[0].value.upper():
            # with CTE, but not iterative
            if "RECURSIVE" == tokens[1].value.upper():
                # CTE and iterative
                self.main_table = sql_metadata.get_query_tables(self.query)[0].title().lower()
                self.is_iter = True

        if self.is_iter:
            cte_pattern = r"(WITH RECURSIVE\s+(.*?)\s*\((.*?)\)\s*AS\s*\()"
            base_case_pattern = r"(\bSELECT\b.*?)(?=\b(?:UNION ALL|UNION|INTERSECT|EXCEPT)\b)"
            recursive_case_pattern = r"(\b(?:UNION ALL|UNION|INTERSECT|EXCEPT)\b(.*?))(?=\))"
            final_query_pattern = r"(?<=\))\s*(SELECT\s+[\s\S]*?)(?=;)"

            cte_groups = re.search(cte_pattern, query, re.DOTALL | re.IGNORECASE)
            self.cte = cte_groups.group(2)
            self.cte_params = cte_groups.group(3).split(",")
            for i in range(len(self.cte_params)):
                self.cte_params[i] = self.cte_params[i].strip()

            self.base = re.search(base_case_pattern, self.query, re.DOTALL | re.IGNORECASE).group(1) + ";"
            self.iterative = re.search(recursive_case_pattern, self.query, re.DOTALL | re.IGNORECASE).group(2) + ";"
            self.final = re.search(final_query_pattern, self.query, re.DOTALL | re.IGNORECASE).group(0) + ";"

        return self

    def is_iterative(self):
        return self.is_iter
