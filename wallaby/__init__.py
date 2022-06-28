from typing import List, Union

import os
import json
import sqlite3
import sys
import time

import pandas as pd

_DEFAULT_SQLITE_PATH = "~/wallaby.db"


class Wallaby:
    def __init__(self, sqlite_database_path: str = _DEFAULT_SQLITE_PATH):
        """
        Create a new pointer to a Wallaby database.

        Arguments:
            sqlite_database_path (str): Path for the Wallaby database. Defaults
                to wallaby._DEFAULT_SQLITE_PATH.

        Returns:
            None

        """
        self._columns = {
            "jobtext": "TEXT",
            "tagscsv": "TEXT",
            "date": "FLOAT",
            "results": "JSON",
        }
        self._sqlite_database_path = os.path.expanduser(sqlite_database_path)
        self._conn = None
        self._initialize()

    def _cursor(self):
        self._conn = self._conn or sqlite3.connect(self._sqlite_database_path)
        return self._conn.cursor()

    def _execute(self, query: str, *args, **kwargs):
        results = self._cursor().execute(query, *args, **kwargs)
        self._conn.commit()
        return results

    def _initialize(self):
        """
        Provision the Wallaby database and prepare to accept results.

        Arguments:
            None

        Returns:
            Bool: True if successful.

        """
        kv = ",\n".join(f"{k} {v}" for k, v in self._columns.items())
        new_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY,
            {kv}
        );
        """
        self._execute(new_table_cmd)
        return True

    def log(
        self, results: Union[dict, str], tags: List[str] = None, jobtext: str = None
    ):
        jobtext = jobtext or " ".join(sys.argv[:])
        tagscsv = "," + (",".join(tags) if tags else "") + ","
        date = time.time()
        if isinstance(results, str):
            results = {"output": results}
        results = json.dumps(results)
        self._execute(
            """
            INSERT INTO
                results(jobtext, tagscsv, date, results)
            VALUES(?, ?, ?, ?)
        """,
            (jobtext, tagscsv, date, results),
        )
        return True

    def get_by_tag(
        self,
        all_of: Union[List[str], str] = None,
        any_of: Union[List[str], str] = None,
        as_dataframe: bool = False,
    ) -> List[tuple]:
        """
        Query for a list of results based upon the tag.

        Arguments:
            all_of (Union[List[str], str]): A list of tags to search for, all
                of which must be included in order for the row to be returned
            any_of (Union[List[str], str]): If any tag in this list is included
                then the row will be returned.
            as_dataframe (bool: False): Whether to return results as DataFrame.

        Returns:
            List[tuple]: A list of SQL rows

        """
        if all_of:
            set_operation = " AND "
        elif any_of:
            set_operation = " OR "
        else:
            raise ValueError("You must specify exactly one of `any_of` or `all_of`.")

        taglist = all_of if all_of else any_of

        if isinstance(taglist, str):
            taglist = [taglist]
        tag_clause = set_operation.join([f"tagscsv LIKE '%,{t},%'" for t in taglist])
        results = self._execute(
            f"""
            SELECT * FROM results WHERE {tag_clause}
        """
        ).fetchall()
        if as_dataframe:
            return pd.DataFrame(results, columns=["id", *self._columns.keys()])
        else:
            return results

    def get_results_since(
        self, since: float, as_dataframe: bool = False
    ) -> List[tuple]:
        """
        Get all results since a timestamp.

        Arguments:
            since (float): The since-time, e.g. time.time()
            as_dataframe (bool: False): Whether to return results as DataFrame.

        Returns:
            List[tuple]: A list of SQL rows

        """
        results = self._execute(
            """
            SELECT * FROM results WHERE date > (?)
        """,
            (since,),
        ).fetchall()
        if as_dataframe:
            return pd.DataFrame(results, columns=["id", *self._columns.keys()])
        else:
            return results

    def raw_query(self, query: str, as_dataframe: bool = False) -> List[tuple]:
        """
        Allow a raw query against the database.

        Arguments:
            query (str): A SQL query against the table
            as_dataframe (bool: False): Whether to return results as DataFrame

        Returns:
            List[tuple]: A list of rows

        """
        results = self._execute(query)

        if as_dataframe:
            return pd.DataFrame(results, columns=["id", *self._columns.keys()])
        else:
            return results


def cli():
    """
    The command-line version of Wallaby. See Readme for usage.
    """

    import subprocess
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--command", default="")
    parser.add_argument("-t", "--tags", default="")
    parser.add_argument("-f", "--database-file", default=_DEFAULT_SQLITE_PATH)
    args = parser.parse_args()

    environment = dict(os.environ)

    if not sys.stdin.isatty():
        stdin = sys.stdin.read()
    else:
        stdin = ""

    if args.command:
        p = subprocess.check_output(
            args.command, shell=True, env=environment, input=stdin
        )
        output = p.decode()
    else:
        output = stdin

    # Construct a nicely organized results dict of this execution:
    result = {
        "environment": environment,
        "output": output,
        "command": args.command or None,
    }

    other_tags = args.tags.split(",") if args.tags else []

    w = Wallaby(args.database_file)
    w.log(result, tags=["cli", *other_tags], jobtext=args.command or None)
    print(output, end="")


def wallaby2json():
    """
    The command-line version of Wallaby. See Readme for usage.
    """

    import subprocess
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--all-tags", default="")
    parser.add_argument("-e", "--include-env", default=False, action="store_true")
    parser.add_argument("-f", "--database-file", default=_DEFAULT_SQLITE_PATH)
    args = parser.parse_args()

    w = Wallaby(args.database_file)
    all_tags = args.all_tags.split(",")
    res = w.get_by_tag(all_of=all_tags, as_dataframe=True)
    res.results = res.results.map(lambda x: json.loads(x))
    res = res.join(pd.json_normalize(res.results))
    res.drop(columns="results", inplace=True)
    if not args.include_env:
        res.drop(
            columns=[col for col in res.columns if col.startswith("environment.")],
            inplace=True,
        )
    print(res.to_json())
