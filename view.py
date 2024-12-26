from model import Model

from tabulate import tabulate

# Class to handle user interaction via console for various database operations
class View:
    # Display a simple message to the user
    def show_message(self, message):
        print(message)
        
    # Display tabular data in a formatted table using 'tabulate'
    def show_data(self, data, columns):
        print(tabulate(data, headers=columns, tablefmt="psql"))
        
    # Get input from the user for inserting data into a table
    def get_insert_input(self):
        table = input("Enter table name: ")
        
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        data = input("Enter data separated by space: ")
        data = data.split()
        
        return table, columns, data
    
    # Get input from the user for viewing data from a table
    def get_table_name(self):
        return input("Enter table name: ")  
    
    # Get input from the user for selecting columns to view
    def get_columns_input(self):
        # Prompt for column names and split them into a list
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        return columns
    
    # Get input from the user for selecting a condition to view data
    def get_condition_input(self):
        # Prompt for the SQL condition (WHERE clause)
        condition = input("Enter condition in postgres SQL (... WHERE [condition]). If not applicable leave empty: ")
        if condition == "":
            condition = None
        return condition
    
    # Get input from the user for updating data in a table
    def get_update_input(self):
        # Prompt for the table name
        table = input("Enter table name: ")
        
        # Prompt for the data to update and split it into a list
        data = input("Enter data separated by space: ")
        data = data.split()
        
        # Prompt for the SQL condition (WHERE clause)
        condition = input("Enter condition in postgres SQL (... WHERE [condition]). If not applicable leave empty: ")
        if condition == "":
            condition = None
        return table, data, condition
    
    # Get input from the user for deleting data from a table
    def get_delete_input(self):
        # Prompt for the table name
        table = input("Enter table name: ")
        
        # Prompt for the SQL condition (WHERE clause)
        condition = input("Enter condition in postgres SQL (... WHERE [condition]). Can not be empty: ")
        if condition == "":
            raise ValueError("Condition can not be empty!")  # Raise an error if no condition is provided
        return table, condition
    
    # Get input from the user for creating a new table
    def get_create_input(self):
        # Prompt for the table name
        table = input("Enter table name: ")
        
        # Prompt for column names and split them into a list
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        # Prompt for data types of columns and split them into a list
        data_types = input("Enter data types separated by space: ")
        data_types = data_types.split()
        
        return table, columns, data_types
    
    # Get input from the user for dropping a table
    def get_drop_input(self):
        # Prompt for the table name
        table = input("Enter table name: ")
        return table
    
    # Get input from the user for generating random data in a table
    def get_generate_random_input(self):
        # Prompt for the table name
        table = input("Enter table name: ")
        
        # Prompt for column names and split them into a list
        columns = input("Enter columns separated by space: ")
        columns = columns.split()
        
        # Prompt for data types of columns and split them into a list
        data_types = input("Enter data types separated by space: ")
        data_types = data_types.split()
        
        # Prompt for additional parameters and convert to a list of tuples
        parameters = input("Enter parameters separated by space: ")
        parameters = parameters.split()
        parameters = [tuple(parameter.split(",")) for parameter in parameters]
        
        # Prompt for the number of rows to generate and validate as integer
        rows_number = input("Enter number of rows: ")
        try:
            rows_number = int(rows_number)
        except ValueError:
            raise ValueError("Number of rows must be integer!")
            
        # Prompt for the length of text columns, with default value if not provided
        text_len = input("Enter length of text columns: ")
        text_len = int(text_len if text_len != "" else 0)
        
        return table, columns, data_types, parameters, rows_number, text_len
    
    # Get input from the user for finding data based on specific conditions
    def get_find_input(self):
        # Prompt for the table name
        table = input("Enter table name: ")
        
        # Prompt for the column name
        column = input("Enter column name: ")
        
        # Initialize condition as empty
        condition = ""
        
        # Prompt for the search type and build the condition accordingly
        t = input("Enter search type (number, string, boolean, date): ")
        if t == "number":
            left = input("Enter left bound: ")
            right = input("Enter right bound: ")
            condition = f"{column} BETWEEN {left} AND {right}"
        elif t == "string":
            string = input("Enter regex string: ")
            condition = f"{column} LIKE '%{string}%'"
        elif t == "boolean":
            boolean = input("Enter boolean value (True, False): ")
            condition = f"{column} = {boolean}"
        elif t == "date":
            left = input("Enter left bound (YYYY-MM-DD): ")
            right = input("Enter right bound (YYYY-MM-DD): ")
            condition = f"{column} BETWEEN '{left}' AND '{right}'"
        
        # Default to None if no condition is created
        if condition == "":
            condition = None
        return table, column, condition
    
    # Get input from the user to calculate total income for payment systems
    def get_pay_systems_total_income_input(self):
        # Prompt for the range of numbers
        left = input("Enter left bound (starting number): ")
        right = input("Enter right bound (last number): ")
        return left, right
    
    # Get input from the user to find company orders within a specific period
    def get_company_orders_thru_period_input(self):
        # Prompt for the start and end dates
        left = input("Enter left bound (starting date YYYY-MM-DD): ")
        right = input("Enter right bound (last date YYYY-MM-DD): ")
        return left, right
    
    # Get input from the user to find the top 5 orders by total price for a company
    def get_top_5_orders_total_price_input(self):
        # Prompt for the company name
        company = input("Enter company name: ")
        return company
