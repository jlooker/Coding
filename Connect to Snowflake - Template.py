"""
    The "<DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook" pulls <DATASET_NAME> data from <DATA_SOURCE_NAME> and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

    Data Pipeline Process:

        Step 1: Install Required Python Packages
        
        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials and connect to the Snowflake data warehouse

        Step 4: Establish a cursor

        Step 5: Query the data from the Snowflake data warehouse

        Step 6: Load the data from the SQL query into a Pandas DataFrame

        Step 7: ENTER THE DATA LOAD LOGIC HERE
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
        # This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks
##############################################################################################################


# Install the snowflake-connector-python[pandas] Python package
    # Used to connect to Snowflake and utilize Pandas DataFrames	
%pip install snowflake-connector-python[pandas]

# Restart the kernel to use updated packages
    # Databricks specific command that is required to use any newly install Python packages listed above
%restart_python


####################################################################################################
# Step 2: Install Required Python Libraries
    # Install all of the require Python libraries in order to perform the necessary actions within the Python notebook
####################################################################################################


# Enables the ability to connect to the Snowflake Data Warehouse
import snowflake.connector

# Enables the ability to write data into Snowflake from Pandas DataFrames
from snowflake.connector.pandas_tools import write_pandas


####################################################################################################
# Step 3: Setup the credentials and connect to the Snowflake data warehouse
    # Setup the credentials to connect to the Snowflake data warehouse
    # Connect to the Snowflake data warehouse
####################################################################################################


# Connect to the Snowflake Data Warehouse using the credentials specified above
    # The connection to the Snowflake data warehouse closes automatically after the with block is exited
        # Therefore, no need for the dw_conn.close() command
with snowflake.connector.connect(
    # The username should be tied to a service account, rather than and specific individual
    user = "<Snowflake_User_Name>"
    # The password should be tied to a service account, rather than and specific individual
    ,password = "<Snowflake_User_Password>"
    # The account consists of 3 parts separated by a decimal (".")
        # Part 1: Snowflake account identifier
        # Part 2: Snowflake cloud region
        # Part 3: Snowflake cloud provider
    ,account = "<Snowflake_Account_Identifier>.<Snowflake_Cloud_Region>.<Snowflake_Cloud_Provider>"
    # The Snowflake warehouse that will be used to write the data into Snowflake
    ,warehouse = "<Snowflake_Warehouse_Name>"
    # The Snowflake database where the data will be written into
    ,database = "<Snowflake_Database_Name>"
    # Enables the use of Multi-Factor Authentication
        # Remove if not using MFA with Snowflake login
    ,passcode = "<MFA_Passcode>"
) as dw_conn:


    ####################################################################################################
    # Step 4: Establish a cursor
    ####################################################################################################


    # Setup a cursor in order to execute SQL queries to retrieve data from the Snowflake data warehouse
        # The cursor closes automatically after the with block is exited
            # Therefore, no need for the cursor.close() command
    with dw_conn.cursor() as cur:


        ####################################################################################################
        # Test the connection to Snowflake
            # Comment out once the connection has been verified
        ####################################################################################################

        
        """
        # Write and execute the SQL query that will return the current version of the Snowflake data warehouse
            # Identifying the current version of the Snowflake data warehouse ensures a successful connection to the Snowflake data warehouse
        cur.execute('''
            SELECT current_version()
        ''')

        # Fetch the first row/record from the SQL query above
        one_row = cur.fetchone()
        
        # Display the first row/record from the SQL query above
        print(one_row[0])
        """


        ####################################################################################################
        # Step 5: Query the data from the Snowflake data warehouse
        ####################################################################################################


        # There are multiple ways to write and execute SQL queries using the cursor
            # Method 1 is preferred, but not required
                # Choose which method you prefer and remove the other


        # Method 1: Write and execute the SQL query in 1 part
        
        # Write and execute the SQL query
        cur.execute("""
            SELECT
                <Column_Name_1>
                ,<Column_Name_2>
                ,<Column_Name_N>
            
            FROM <Table_Name>
        """)


        # Method 2: Write and execute the SQL query in 2 parts
        
        # Part 1: Write the SQL query
        sql = """
            SELECT
                <Column_Name_1>
                ,<Column_Name_2>
                ,<Column_Name_N>
            
            FROM <Table_Name>
        """

        # Part 2: Execute the SQL query from above
        cur.execute(sql)


        ####################################################################################################
        # Step 6: Load the data from the SQL query into a Pandas DataFrame
        ####################################################################################################


        # Fetch the result set from the cursor and deliver it as the pandas DataFrame.
        df = cur.fetch_pandas_all()

        # Display the data within the Pandas DataFrame
            # Remove once the data has been verified
        print(df)
    

####################################################################################################
# Step 7: ENTER THE DATA LOAD LOGIC HERE
####################################################################################################


# ENTER THE DATA LOAD LOGIC HERE FOR LOADING THE DATA INTO THE DESTINATION LOCATION