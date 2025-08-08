import os
import csv
import mysql.connector
import pandas as pd
from datetime import datetime
from tabulate import tabulate
from mysql.connector import errorcode

class DatabaseComparator:
    def __init__(self, instance1_config, instance2_config, db_name):
        self.instance1_config = instance1_config
        self.instance2_config = instance2_config
        self.db_name          = db_name
        self.conn1            = None
        self.conn2            = None
        self.report_time      = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.report_dir       = f"db_comparison_reports_{self.report_time}"
        
        # Create report directory if it doesn't exist
        os.makedirs(self.report_dir, exist_ok=True)
    
    def connect(self):
        """
        Establish connections to both database instances.

        This method will establish database connections using the provided
        connection configurations for both instances. The method will attempt
        to connect to the database specified by `db_name` in both instances.
        If either connection fails, an error message will be printed and
        the method will raise the caught exception.

        """
        try:
            print("\nConnecting to Instance 1...")
            self.conn1 = mysql.connector.connect(**self.instance1_config, database=self.db_name)
            print("Connected to Instance 1 successfully.")
            
            print("\nConnecting to Instance 2...")
            self.conn2 = mysql.connector.connect(**self.instance2_config, database=self.db_name)
            print("Connected to Instance 2 successfully.")
            
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Error: Access denied for one or both instances")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print(f"Error: Database {self.db_name} does not exist on one or both instances")
            else:
                print(f"Error: {err}")
            raise
    
    def disconnect(self):
        """
        Close database connections.

        This method will close both database connections that were established
        in the `connect` method. If either connection is not active, the method
        does nothing.

        """
        if self.conn1 and self.conn1.is_connected():
            self.conn1.close()
        if self.conn2 and self.conn2.is_connected():
            self.conn2.close()
    
    def get_tables(self, connection):
        """
        Retrieve a list of tables in the database.

        This function executes a SHOW TABLES query to fetch the names
        of all tables within the connected database instance.

        Parameters:
        connection (mysql.connector.connect): An established database connection.

        Returns:
        list: A list of table names present in the database.
        """
        cursor = connection.cursor()
        try:
            # Execute the query to get all table names
            cursor.execute("SHOW TABLES")
            # Fetch all results and extract table names from tuples
            return [table[0] for table in cursor.fetchall()]
        except mysql.connector.Error as err:
            # Print error message if query fails
            print(f"Error fetching tables: {err}")
            return []
    
    def get_table_schema(self, connection, table_name):
        """
        Get column details for a specific table.

        This function executes a SHOW COLUMNS query against the specified table
        and returns the results as a list of dictionaries, where each dictionary
        represents a column definition.

        Parameters:
        connection (mysql.connector.connect): An established database connection.
        table_name (str): The name of the table to retrieve column details from.

        Returns:
        list: A list of dictionaries containing column definitions.
        """
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            return cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"Error getting schema for table {table_name}: {err}")
            return None
    
    def get_primary_keys(self, connection, table_name):
        """
        Get primary key columns for a specified table.

        This function retrieves the primary key columns of the given table
        from the connected MySQL database instance.

        Parameters:
        connection (mysql.connector.connect): An established database connection.
        table_name (str): The name of the table to retrieve primary keys from.

        Returns:
        list: A list of column names that are part of the primary key.
        """
        # Create a cursor with dictionary output format
        cursor = connection.cursor(dictionary=True)
        try:
            # Execute query to get primary key columns
            cursor.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")

            # Fetch all results and extract column names
            keys = cursor.fetchall()
            return [key['Column_name'] for key in keys]

        except mysql.connector.Error as err:
            # Handle and print any errors that occur during the query
            print(f"Error getting primary keys for table {table_name}: {err}")
            return []
    
    def get_table_data(self, connection, table_name):
        """
        Get all data from a table as a DataFrame.

        This function executes a SQL query to retrieve all data from a specified table
        and returns it as a Pandas DataFrame.

        Parameters:
        connection (mysql.connector.connect): An established database connection.
        table_name (str): The name of the table to retrieve data from.

        Returns:
        pd.DataFrame: A DataFrame containing all data from the specified table.
        """
        try:
            query = f"SELECT * FROM `{table_name}`"
            return pd.read_sql(query, connection)
        except Exception as e:
            print(f"Error fetching data from table {table_name}: {str(e)}")
            return None
    
    def compare_schemas(self):
        """
        Compare the tables present in each database instance and generate a report.

        This function performs the following steps:
        1. Retrieves the list of tables from both database instances.
        2. Identifies tables that are unique to each instance and those common to both.
        3. Generates a detailed report highlighting table presence differences.
        4. Displays the report in the console and saves it as a CSV file.

        Returns:
            list: A list of tables common to both database instances.
        """
        print("\n=== Comparing database schemas ===")

        # Get tables from both instances
        tables1 = set(self.get_tables(self.conn1))
        tables2 = set(self.get_tables(self.conn2))

        # Check if there are no tables in both instances
        if not tables1 and not tables2:
            print("No tables found in either database.")
            return []

        # Find differences
        only_in_instance1 = tables1 - tables2
        only_in_instance2 = tables2 - tables1
        common_tables = tables1 & tables2

        # Prepare report data
        schema_report = []

        # Add tables present only in Instance 1
        for table in sorted(only_in_instance1):
            schema_report.append({
                "Table": table,
                "Instance 1": "Present",
                "Instance 2": "Missing",
                "Difference": "Table missing in Instance 2"
            })

        # Add tables present only in Instance 2
        for table in sorted(only_in_instance2):
            schema_report.append({
                "Table": table,
                "Instance 1": "Missing",
                "Instance 2": "Present",
                "Difference": "Table missing in Instance 1"
            })

        # Add tables present in both instances
        for table in sorted(common_tables):
            schema_report.append({
                "Table": table,
                "Instance 1": "Present",
                "Instance 2": "Present",
                "Difference": "None"
            })

        # Display report
        print("\nSchema Comparison Results:")
        print(tabulate(schema_report, headers="keys", tablefmt="grid", showindex=False))

        # Save report to CSV
        csv_path = os.path.join(self.report_dir, "1_schema_comparison.csv")
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=schema_report[0].keys())
            writer.writeheader()
            writer.writerows(schema_report)
        print(f"\nSchema comparison report saved to: {csv_path}")

        return common_tables
    
    def compare_columns(self, common_tables):
        """
        Compare column structure of common tables

        Iterate through the common tables and:
        1. Get column info from both instances
        2. Find differences (column names, types, nullability, keys, defaults, and extras)
        3. Categorize differences
        4. Prepare a report for each table
        """
        print("\n=== Comparing table columns ===")
        column_reports = []
        tables_with_column_differences = 0
        
        for table in common_tables:
            print(f"\nProcessing table: {table}")
            
            # Get column info from both instances
            columns1 = self.get_table_schema(self.conn1, table)
            columns2 = self.get_table_schema(self.conn2, table)
            
            if columns1 is None or columns2 is None:
                print(f"Skipping column comparison for table {table} due to previous errors")
                continue
            
            # Convert column info to dictionaries for easier comparison
            columns1_dict = {col['Field']: col for col in columns1}
            columns2_dict = {col['Field']: col for col in columns2}
            
            # Find all columns in both instances
            all_columns = set(columns1_dict.keys()).union(set(columns2_dict.keys()))
            table_has_differences = False
            table_column_report = []
            
            # Compare columns
            for col in sorted(all_columns):
                col1 = columns1_dict.get(col)
                col2 = columns2_dict.get(col)
                
                # Column only in one instance
                if col1 and not col2:
                    table_column_report.append({
                        "Table": table,
                        "Column": col,
                        "Status": "Only in Instance 1",
                        "Type (Instance 1)": col1['Type'],
                        "Null (Instance 1)": col1['Null'],
                        "Key (Instance 1)": col1['Key'],
                        "Default (Instance 1)": col1['Default'],
                        "Extra (Instance 1)": col1['Extra'],
                        "Type (Instance 2)": "N/A",
                        "Null (Instance 2)": "N/A",
                        "Key (Instance 2)": "N/A",
                        "Default (Instance 2)": "N/A",
                        "Extra (Instance 2)": "N/A",
                        "Difference": "Column missing in Instance 2"
                    })
                    table_has_differences = True
                elif col2 and not col1:
                    table_column_report.append({
                        "Table": table,
                        "Column": col,
                        "Status": "Only in Instance 2",
                        "Type (Instance 1)": "N/A",
                        "Null (Instance 1)": "N/A",
                        "Key (Instance 1)": "N/A",
                        "Default (Instance 1)": "N/A",
                        "Extra (Instance 1)": "N/A",
                        "Type (Instance 2)": col2['Type'],
                        "Null (Instance 2)": col2['Null'],
                        "Key (Instance 2)": col2['Key'],
                        "Default (Instance 2)": col2['Default'],
                        "Extra (Instance 2)": col2['Extra'],
                        "Difference": "Column missing in Instance 1"
                    })
                    table_has_differences = True
                else:
                    # Column exists in both, compare properties
                    differences = []
                    if col1['Type'] != col2['Type']:
                        differences.append(f"Type({col1['Type']} vs {col2['Type']})")
                    if col1['Null'] != col2['Null']:
                        differences.append(f"Null({col1['Null']} vs {col2['Null']})")
                    if col1['Key'] != col2['Key']:
                        differences.append(f"Key({col1['Key']} vs {col2['Key']})")
                    if str(col1['Default']) != str(col2['Default']):
                        differences.append(f"Default({col1['Default']} vs {col2['Default']})")
                    if col1['Extra'] != col2['Extra']:
                        differences.append(f"Extra({col1['Extra']} vs {col2['Extra']})")
                    
                    if differences:
                        table_column_report.append({
                            "Table": table,
                            "Column": col,
                            "Status": "Different",
                            "Type (Instance 1)": col1['Type'],
                            "Null (Instance 1)": col1['Null'],
                            "Key (Instance 1)": col1['Key'],
                            "Default (Instance 1)": col1['Default'],
                            "Extra (Instance 1)": col1['Extra'],
                            "Type (Instance 2)": col2['Type'],
                            "Null (Instance 2)": col2['Null'],
                            "Key (Instance 2)": col2['Key'],
                            "Default (Instance 2)": col2['Default'],
                            "Extra (Instance 2)": col2['Extra'],
                            "Difference": ", ".join(differences)
                        })
                        table_has_differences = True
            
            if table_has_differences:
                column_reports.extend(table_column_report)
                tables_with_column_differences += 1
                print(f"Found {len(table_column_report)} column differences in table {table}")
            else:
                print(f"No column differences found in table {table}")
        
        if column_reports:
            # Display summary
            print(f"\nFound column differences in {tables_with_column_differences} tables")
            
            # Save to CSV
            csv_path = os.path.join(self.report_dir, "2_column_comparison.csv")
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=column_reports[0].keys())
                writer.writeheader()
                writer.writerows(column_reports)
            print(f"Column comparison report saved to: {csv_path}")
        else:
            print("\nNo column differences found in any tables.")
    
    def compare_row_data(self, common_tables):
        """
        Compare row-level data in common tables

        Iterate through the common tables and:
        1. Get primary key columns
        2. Get all data from both instances
        3. Find differences (rows only in one instance or with different values)
        4. Categorize differences
        5. Prepare a report for each table
        """
        print("\n=== Comparing row data ===")
        data_reports = []
        tables_with_data_differences = 0
        
        for table in common_tables:
            print(f"\nProcessing table: {table}")
            
            try:
                # Get primary key columns
                pk_columns = self.get_primary_keys(self.conn1, table)
                if not pk_columns:
                    print(f"Warning: Table {table} has no primary key. Skipping data comparison.")
                    continue
                
                # Get all data from both instances
                df1 = self.get_table_data(self.conn1, table)
                df2 = self.get_table_data(self.conn2, table)
                
                if df1 is None or df2 is None:
                    print(f"Skipping data comparison for table {table} due to previous errors")
                    continue
                
                # Find differences
                merged = pd.merge(
                    df1, 
                    df2, 
                    on=pk_columns, 
                    how='outer', 
                    suffixes=('_1', '_2'), 
                    indicator=True
                )
                
                # Categorize differences
                only_in_instance1 = merged[merged['_merge'] == 'left_only'][pk_columns]
                only_in_instance2 = merged[merged['_merge'] == 'right_only'][pk_columns]
                in_both = merged[merged['_merge'] == 'both']
                
                # Find rows with different values (excluding PK columns)
                data_columns = [col for col in df1.columns if col not in pk_columns]
                diff_mask = False
                for col in data_columns:
                    # Handle NaN comparisons properly
                    col1 = in_both[f"{col}_1"]
                    col2 = in_both[f"{col}_2"]
                    diff_mask |= (
                        (col1 != col2) & 
                        ~(col1.isna() & col2.isna())
                    )
                
                different_rows = in_both[diff_mask]
                
                # Prepare report
                table_has_differences = False
                table_data_report = []
                
                # Rows only in instance 1
                for _, row in only_in_instance1.iterrows():
                    table_data_report.append({
                        "Table": table,
                        "Primary Key": self.format_pk(row[pk_columns]),
                        "Status": "Only in Instance 1",
                        "Column": "ALL",
                        "Value (Instance 1)": "Exists",
                        "Value (Instance 2)": "Missing",
                        "Difference": "Row missing in Instance 2"
                    })
                    table_has_differences = True
                
                # Rows only in instance 2
                for _, row in only_in_instance2.iterrows():
                    table_data_report.append({
                        "Table": table,
                        "Primary Key": self.format_pk(row[pk_columns]),
                        "Status": "Only in Instance 2",
                        "Column": "ALL",
                        "Value (Instance 1)": "Missing",
                        "Value (Instance 2)": "Exists",
                        "Difference": "Row missing in Instance 1"
                    })
                    table_has_differences = True
                
                # Rows with different values
                for _, row in different_rows.iterrows():
                    for col in data_columns:
                        val1 = row[f"{col}_1"]
                        val2 = row[f"{col}_2"]
                        
                        # Skip if values are equal or both NaN
                        if (val1 == val2) or (pd.isna(val1) and pd.isna(val2)):
                            continue
                            
                        table_data_report.append({
                            "Table": table,
                            "Primary Key": self.format_pk(row[pk_columns]),
                            "Status": "Different Values",
                            "Column": col,
                            "Value (Instance 1)": str(val1),
                            "Value (Instance 2)": str(val2),
                            "Difference": f"Different values ({str(val1)} vs {str(val2)})"
                        })
                        table_has_differences = True
                
                if table_has_differences:
                    data_reports.extend(table_data_report)
                    tables_with_data_differences += 1
                    print(f"Found {len(table_data_report)} data differences in table {table}")
                else:
                    print(f"No data differences found in table {table}")
            
            except Exception as e:
                print(f"Error comparing data in table {table}: {str(e)}")
                continue
        
        if data_reports:
            # Display summary
            print(f"\nFound data differences in {tables_with_data_differences} tables")
            
            # Save to CSV
            csv_path = os.path.join(self.report_dir, "3_data_comparison.csv")
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data_reports[0].keys())
                writer.writeheader()
                writer.writerows(data_reports)
            print(f"Data comparison report saved to: {csv_path}")
        else:
            print("\nNo row-level data differences found in any tables.")
    
    def format_pk(self, pk_values):
        """
        Format primary key values for display.

        Parameters:
        pk_values (pd.Series, list, tuple, or other): The primary key values to format.

        Returns:
        str: A string representation of the primary key values.
        """
        # Check if pk_values is a Pandas Series
        if isinstance(pk_values, pd.Series):
            # Convert Series values to a comma-separated string
            return ", ".join([str(v) for v in pk_values.values])

        # Check if pk_values is a list or tuple
        elif isinstance(pk_values, (list, tuple)):
            # Convert list or tuple values to a comma-separated string
            return ", ".join([str(v) for v in pk_values])

        # For any other type, convert directly to string
        else:
            return str(pk_values)
    
    def run_comparison(self):
        """
        Run all comparison steps

        This method executes the following steps in order:
        1. Connect to both database instances
        2. Compare database schemas (tables that exist in each instance)
        3. Compare column structures for common tables
        4. Compare row-level data for tables with identical structures

        If any errors occur, the method will catch the exception, print an error message, and
        disconnect from both database instances.
        """
        try:
            self.connect()
            
            # Schema comparison
            common_tables = self.compare_schemas()
            
            if common_tables:
                # Column comparison
                self.compare_columns(common_tables)
                
                # Data comparison
                self.compare_row_data(common_tables)
            
            print("\n=== Comparison completed successfully! ===")
            print(f"Reports saved in directory: {self.report_dir}")
            
        except Exception as e:
            print(f"\nError during comparison: {str(e)}")
            print("Comparison incomplete due to errors.")
        finally:
            self.disconnect()

if __name__ == "__main__":
    # Configuration for your  MySQL instances
    # Replace these with your actual connection details

    instance1_config = {
        'host'             : 'instance1-ip-or-hostname',
        'user'             : 'your-username',
        'password'         : 'your-password',
        'port'             : 3306,
        'raise_on_warnings': True
    }
    
    instance2_config = {
        'host'             : 'instance2-ip-or-hostname',
        'user'             : 'your-username',
        'password'         : 'your-password',
        'port'             : 3306,
        'raise_on_warnings': True
    }

    db_name = 'database_name_to_compare'
    
    print("===  MySQL Database Comparison Tool ===")
    print(f"Comparing database '{db_name}' between two instances")
    
    # Create and run comparator
    comparator = DatabaseComparator(instance1_config, instance2_config, db_name)
    comparator.run_comparison()
