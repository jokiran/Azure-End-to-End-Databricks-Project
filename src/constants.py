CATALOG_NAME = "databricks_project1"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

EMPLOYEE_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.employee_payroll"
LABOR_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.labor_position"
FACILITY_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.facility"


EMPLOYEE_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.employee_payroll"
LABOR_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.labor_position"
FACILITY_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.facility"

LANDING_PATH = "/Volumes/databricks_project1/bronze/landing_volume"

EMPLOYEE_FILE = f"{LANDING_PATH}/Employee_Payroll.xlsx"
LABOR_FILE = f"{LANDING_PATH}/Labor_Position.xlsx"