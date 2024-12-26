from controller import Controller

DB_NAME = "client-management-system-for-companies"
USER = "postgres"
HOST = "localhost"
PASSWORD = "1111"

def main():
    controller = Controller(DB_NAME, USER, PASSWORD, HOST)
    controller.run()

if __name__ == "__main__":
    main()
