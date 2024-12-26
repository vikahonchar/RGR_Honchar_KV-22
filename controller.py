from model import Model
from view import View

# Controller class to connect the model and view, managing the application's operations
class Controller:
    def __init__(self, db_name, user, password, host):
        # Initialize model and view objects
        self.model = Model(db_name, user, password, host)
        self.view = View()

    # Main loop to run the application
    def run(self):
        while True:
            self.show_tables()  # Display available tables
            choice = self.show_menu()  # Show menu and get user's choice
            if choice == "1":
                self.insert_data()
            elif choice == "2":
                self.view_data()
            elif choice == "3":
                self.update_data()
            elif choice == "4":
                self.delete_data()
            elif choice == "5":
                self.create_table()
            elif choice == "6":
                self.drop_table()
            elif choice == "7":
                self.generate_random_data()
            elif choice == "8":
                self.find_data()
            elif choice == "9":
                a = self.show_algorithms()  # Show algorithms submenu
                if a == "1":
                    self.pay_systems_total_income()
                elif a == "2":
                    self.company_orders_thru_period()
                elif a == "3":
                    self.top_5_orders_total_price()
                elif a == "0":
                    continue  # Return to main menu
            elif choice == "0":
                break  # Exit the application
            else:
                self.view.show_message("Invalid choice!")
                
    # Display available tables
    def show_tables(self):
        tables = self.model.get_tables()  # Fetch tables from the database
        tables = [table[0] for table in tables]  # Extract table names
        self.view.show_message(f"\nAvailable tables: {tables if tables is not None else 'None'}")

    # Display the main menu
    def show_menu(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Insert Data")
        self.view.show_message("2. View Data")
        self.view.show_message("3. Update Data")
        self.view.show_message("4. Delete Data")
        self.view.show_message("5. Create Table")
        self.view.show_message("6. Drop Table")
        self.view.show_message("7. Generate Random Data")
        self.view.show_message("8. Find Data")
        self.view.show_message("9. Algorithms")
        self.view.show_message("0. Quit")
        return input("Enter your choice: ")
    
    # Display the algorithms submenu
    def show_algorithms(self):
        self.view.show_message("\nAlgorithms:")
        self.view.show_message("1. Pay Systems' Total Income")
        self.view.show_message("2. Company's Orders' thru Period")
        self.view.show_message("3. Top 5 Orders' Total Price")
        self.view.show_message("0. Quit")
        return input("Enter your choice: ")
        
    # Insert data into a table
    def insert_data(self):
        table, columns, data = self.view.get_insert_input()
        if self.model.insert_data(table, columns, data):
            self.view.show_message("Data inserted successfully!")
        else:
            self.view.show_message("Data insertion failed!")
        
    # View data from a table
    def view_data(self):
        table = self.view.get_table_name()  # Get table name from the user
        columns = self.model.get_columns(table)  # Retrieve column names from the database
        if columns is None:
            self.view.show_message("Failed to retrieve column names.")
            return
        columns = [column[0] for column in columns]  # Extract column names
        self.view.show_message(f"Available columns: {', '.join(columns)}")
        selected_columns = self.view.get_columns_input()  # Get desired columns from the user
        condition = self.view.get_condition_input()  # Get condition from the user
        data = self.model.get_data(table, selected_columns, condition)  # Fetch data from the database
        if data is not None:
            self.view.show_data(data, selected_columns)  # Display the data
        else:
            self.view.show_message("Data retrieval failed!")

    # Update data in a table
    def update_data(self):
        table, data, condition = self.view.get_update_input()  # Get input from the user
        if self.model.update_data(table, data, condition):  # Attempt to update data
            self.view.show_message("Data updated successfully!")
        else:
            self.view.show_message("Data update failed!")

    # Delete data from a table
    def delete_data(self):
        table = self.view.get_table_name()  # Get table name from the user
        columns = self.model.get_columns(table)  # Retrieve column names from the database
        if columns is None:
            self.view.show_message("Failed to retrieve column names.")
            return
        columns = [column[0] for column in columns]  # Extract column names
        self.view.show_message(f"Available columns: {', '.join(columns)}")
        
        condition = self.view.get_condition_input()  # Get condition from the user
        confirm = self.view.get_confirmation(f"Are you sure you want to delete rows matching condition: {condition}? (yes/no): ")
        if confirm.lower() == "yes":
            if self.model.delete_data(table, condition):  # Attempt to delete data
                self.view.show_message("Data deleted successfully!")
            else:
                self.view.show_message("Data deletion failed!")
        else:
            self.view.show_message("Delete operation canceled.")

            
    # Create a new table
    def create_table(self):
        table, columns, data_types = self.view.get_create_input()  # Get input from the user
        if self.model.create_table(table, columns, data_types):  # Attempt to create table
            self.view.show_message("Table created successfully!")
        else:
            self.view.show_message("Table creation failed!")
            
    # Drop an existing table
    def drop_table(self):
        table = self.view.get_drop_input()  # Get input from the user
        if self.model.drop_table(table):  # Attempt to drop table
            self.view.show_message("Table dropped successfully!")
        else:
            self.view.show_message("Table drop failed!")
            
    # Generate random data for a table
    def generate_random_data(self):
        table, columns, data_types, parameters, rows_number, text_len = self.view.get_generate_random_input()  # Get input from the user
        if self.model.generate_random_data(table, columns, data_types, parameters, rows_number, text_len):  # Attempt to generate random data
            self.view.show_message("Random data generated successfully!")
        else:
            self.view.show_message("Random data generation failed!")
            
    # Find data based on specific conditions
    def find_data(self):
        table, column, condition = self.view.get_find_input()  # Get input from the user
        data = self.model.get_data(table, [column], condition)  # Fetch data from the database
        if data is not None:
            self.view.show_data(data, [column])  # Display the data
        else:
            self.view.show_message("Data retrieval failed!")
            
    # Calculate total income for payment systems
    def pay_systems_total_income(self):
        left, right = self.view.get_pay_systems_total_income_input()  # Get input from the user
        data = self.model.pay_systems_total_income(left, right)  # Fetch the data
        if data is not None:
            self.view.show_data(data, ["id", "name", "count", "total_income"])  # Display the data
        else:
            self.view.show_message("Data retrieval failed!")
            
    # Get company orders within a specific period
    def company_orders_thru_period(self):
        left, right = self.view.get_company_orders_thru_period_input()  # Get input from the user
        data = self.model.company_orders_thru_period(left, right)  # Fetch the data
        if data is not None:
            self.view.show_data(data, ["id", "company", "orders"])  # Display the data
        else:
            self.view.show_message("Data retrieval failed!")
            
    # Find the top 5 orders by total price for a specific company
    def top_5_orders_total_price(self):
        company = self.view.get_top_5_orders_total_price_input()  # Get input from the user
        data = self.model.top_5_orders_total_price(company)  # Fetch the data
        if data is not None:
            self.view.show_data(data, ["order_id", "total_price"])  # Display the data
        else:
            self.view.show_message("Data retrieval failed!")
