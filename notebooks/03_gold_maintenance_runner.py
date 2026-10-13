# Databricks notebook source
import os

# 1. Resolve path to src/maintenance/refresh_and_optimize.sql
current_dir = os.path.dirname(os.path.abspath("__file__"))
sql_file_path = os.path.abspath(os.path.join(current_dir, "..", "src", "maintenance", "refresh_and_optimize.sql"))

print(f"Reading maintenance statements from: {sql_file_path}")

with open(sql_file_path, "r") as f:
    sql_script = f.read()

# 2. Split statements by semicolon and execute iteratively
statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]

for idx, statement in enumerate(statements, 1):
    # Skip full-line comment blocks
    clean_lines = [line for line in statement.splitlines() if not line.strip().startswith("--")]
    exec_sql = "\n".join(clean_lines).strip()
    
    if exec_sql:
        first_line = exec_sql.splitlines()[0]
        print(f"[{idx}/{len(statements)}] Executing: {first_line}...")
        spark.sql(exec_sql)

print("SUCCESS: All table maintenance, Z-ORDER, and VACUUM routines completed.")