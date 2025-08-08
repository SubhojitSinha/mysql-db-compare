# MySQL Database Comparison Tool

A comprehensive Python tool for comparing MySQL databases between two different instances. This tool performs detailed comparisons of database schemas, table structures, column definitions, and row-level data differences.

## Features

- **Schema Comparison**: Compare which tables exist in each database instance
- **Column Structure Comparison**: Analyze differences in column definitions, data types, and constraints
- **Row-Level Data Comparison**: Identify differences in actual data between corresponding tables
- **Detailed Reporting**: Generate CSV reports for all comparison results
- **Primary Key Tracking**: Track data differences using primary key references
- **Error Handling**: Robust error handling for connection and query issues

## Prerequisites

- Python 3.7 or higher
- MySQL Server instances (2 instances to compare)
- Network access to both MySQL instances

## Installation

1. Clone or download this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Dependencies

- `mysql-connector-python==9.4.0` - MySQL database connectivity
- `pandas==2.3.1` - Data manipulation and analysis
- `tabulate==0.9.0` - Pretty-print tabular data
- `numpy==2.3.2` - Numerical computing support

## Configuration

Before running the tool, update the database connection configurations in `main.py`:

```python
instance1_config = {
    'host': 'your-instance1-host',
    'user': 'your-username',
    'password': 'your-password',
    'port': 3306,
    'raise_on_warnings': True
}

instance2_config = {
    'host': 'your-instance2-host', 
    'user': 'your-username',
    'password': 'your-password',
    'port': 3306,
    'raise_on_warnings': True
}

db_name = 'your_database_name'
```

## Usage

Run the comparison tool:

```bash
python main.py
```

The tool will:

1. Connect to both MySQL instances
2. Compare database schemas (tables that exist in each instance)
3. Compare column structures for common tables
4. Compare row-level data for tables with identical structures
5. Generate detailed reports in CSV format

## Output Reports

The tool generates timestamped reports in a directory named `db_comparison_reports_YYYYMMDD_HHMMSS/`:

### 1. Schema Comparison (`1_schema_comparison.csv`)
- Tables that exist only in Instance 1
- Tables that exist only in Instance 2
- Tables that exist in both instances

### 2. Column Comparison (`2_column_comparison.csv`)
- Column differences between common tables
- Data type mismatches
- Constraint differences
- Missing columns in either instance

### 3. Data Comparison (`3_data_comparison.csv`)
- Row-level data differences
- Primary key references for changed records
- Specific column values that differ between instances
- New/missing records in either instance

## Example Output

```
=== MySQL Database Comparison Tool ===
Comparing database 'your_database' between two instances

Connecting to Instance 1...
Connected to Instance 1 successfully.

Connecting to Instance 2...
Connected to Instance 2 successfully.

=== Comparing database schemas ===
Found 15 common tables between both instances
Schema comparison report saved to: db_comparison_reports_20250808_165203/1_schema_comparison.csv

=== Comparing column structures ===
Found column differences in 3 tables
Column comparison report saved to: db_comparison_reports_20250808_165203/2_column_comparison.csv

=== Comparing row-level data ===
Found data differences in 2 tables
Data comparison report saved to: db_comparison_reports_20250808_165203/3_data_comparison.csv

=== Comparison completed successfully! ===
Reports saved in directory: db_comparison_reports_20250808_165203
```

## Key Classes and Methods

### DatabaseComparator Class

The main class that handles all comparison operations:

- `__init__(instance1_config, instance2_config, db_name)` - Initialize with connection configs
- `connect()` - Establish connections to both database instances
- `compare_schemas()` - Compare table existence between instances
- `compare_columns(common_tables)` - Compare column structures
- `compare_row_data(common_tables)` - Compare actual data rows
- `run_comparison()` - Execute the complete comparison workflow

## Error Handling

The tool includes comprehensive error handling for:

- Database connection failures
- Authentication errors
- Missing databases or tables
- Query execution errors
- Data type conversion issues

## Security Considerations

- **Never commit database credentials to version control**
- Use environment variables or secure configuration files for production use
- Ensure proper network security between your machine and database instances
- Consider using database users with read-only permissions for comparison tasks

## Limitations

- Large tables may take significant time to compare
- Memory usage scales with table size for data comparisons
- Currently supports MySQL databases only
- Requires identical primary key structures for accurate row-level comparison

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve this tool.

## License

This project is open source. Please check the license file for details.
