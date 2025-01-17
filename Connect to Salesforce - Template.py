"""
    Reference Material:
        - Youtube Video: https://www.youtube.com/watch?v=nPQFUgsk6Oo&t=283s
        - Website: https://satvasolutions.medium.com/salesforce-intwith-python-a-step-by-step-guide-40f9dca7e29b
"""


"""
    The "<DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook" pulls KKC operator review data from <DATA_SOURCE_NAME> and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

    Data Pipeline Process:

        Step 1: Install Required Python Packages

        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to Salesforce

        Step 4: Connect to Salesforce

        Step 5: Query and Extract the data from the Salesforce object

        Step 6: Load the data from the SOQL query into a Pandas DataFrame

        Step 7: Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse
            1. Setup the credentials to connect to the Snowflake data warehouse
            2. Connect to the Snowflake data warehouse
            3. Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
            4. Establish the cur cursor
                1. Assign the proper values for the following Snowflake objects:
                    1. Role
                    2. Warehouse
                    3. Database
                2. Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
                3. Truncate the Snowflake DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
                4. Transform and load the GrubHub Order data into the Snowflake DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
                5. Merge the GrubHub Order data into the Snowflake DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
                6. Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
##############################################################################################################


# Install the datetime Python package
    # Used when working with and manipulating dates and times	
%pip install datetime

# Install the pandas Python package
    # Used to store data in Series and DataFrames	
%pip install pandas

# Install the simple_salesforce Python package
    # Used to connect to Salesforce environments	
%pip install simple_salesforce

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


# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import pandas as pd

# Enables the ability to connect to Salesforce
    # Documentation for simple_salesforce: https://pypi.org/project/simple-salesforce/
from simple_salesforce import Salesforce

# Enables the ability to connect to the Snowflake Data Warehouse
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import snowflake.connector

# Enables the ability to write data into Snowflake from Pandas DataFrames
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
from snowflake.connector.pandas_tools import write_pandas


##############################################################################################################
# Step 3: Setup the credentials to connect to Salesforce
##############################################################################################################


# The URL that is used to identify the Salesforce environment that you wish to connect to
sf_instance_url = "https://krispykrunchychicken.my.salesforce.com/"

# The username of the account that will be used to connect to Salesforce
    # The username should be tied to a service account, rather than and specific individual
sf_user = "joshualooker@krispykrunchy.com"

# The password of the account that will be used to connect to Salesforce
    # The password should be tied to a service account, rather than and specific individual
sf_password = "NJL_CO_2025$"

# The security token of the account that will be used to connect to Salesforce
sf_scecurity_token = "2WFa6UAgGWwM4GSPQdemBjQu"

# The consumer key tied to the Salesforce environment that you wish to connect to
sf_consumer_key = "3MVG9l2zHsylwlpS60YhvCSnXSO0S4D3g9ZDaKt1aEO1PNW.ne8bh59hBqdZNGTOuGw6cuL.bwND8qQDiQleb"

# The consumer secret tied to the Salesforce environment that you wish to connect to
sf_consumer_secret = "77A0DFC387DC2594B6F1099F8090BF75294577934F7FFB4AABCF33174D2145C1"

# The session ID that is used to authentication the Salesforce connection
sf_session_id = ""


##############################################################################################################
# Step 4: Connect to Salesforce
##############################################################################################################


# Connect to Salesforce using the credentials specified above
sf = Salesforce(
    instance_url = sf_instance_url
    ,username = sf_user
    ,password = sf_password
    ,security_token = sf_scecurity_token
    ,consumer_key = sf_consumer_key
    ,consumer_secret = sf_consumer_secret
    ,session_id = sf_session_id
)


##############################################################################################################
# Collect a list of file names for all files within Salesforce
    # This will help identify what data is available to extract within the Salesforce Object
        # Comment out once the field names have been identified
##############################################################################################################


# Create a dictionary to collect Salesforce object metadata
desc_sf_obj = sf.Account.describe()

# Create a list to collect all of the field names from the desc_sf_obj dictionary
field_names = [field['name'] for field in desc_sf_obj['fields']]

# Display all of the field names within the field_names list
    # Comment out once all of the field names have been identified
print()
print("field_names results:")
print()
print(field_names)


####################################################################################################
# Step 5: Query and Extract the data from the Salesforce object
####################################################################################################


# Write the SOQL query that will extract the necessary data from the Salesforce object
soql_query = """
    SELECT
        <Column_1>
        ,<Column_2>
        ,<Column_...N>
    
    FROM Account
"""

# Load the data from the SOQL query into the soql_result variable
soql_result = sf.query_all(soql_query)

# Display the data within the soql_result variable
    # Comment out once the data has been verified
print()
print("soql_result results:")
print()
print(soql_result)


####################################################################################################
# Step 6: Load the data from the SOQL query into a Pandas DataFrame
####################################################################################################


# Load the data from the soql_result variable above into a Pandas DataFrame
df = pd.DataFrame(soql_result.get('records'))

# Display the data within the df Pandas DataFrame
    # Comment out once the data has been verified
print()
print("df results:")
print()
print(df)

# Drop the attributes column, as we do not wish to load this column into Snowflake
df = df.drop(columns = ["attributes"])

# Display the updated data within the Pandas DataFrame with the attributes column removed
    # Comment out once the data has been verified
print()
print("df results with the attributes column removed:")
print()
print(df)


####################################################################################################
# Step 7: Load the data from the df Pandas DataFrame into the Snowflake data warehouse
    # Setup the credentials to connect to the Snowflake data warehouse
    # Connect to the Snowflake data warehouse
    # Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
    # Establish the cur cursor
        # Assign the proper values for the following Snowflake objects:
            # Role
            # Warehouse
            # Database
        # Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
        # Truncate the Snowflake DB_KKF_MAIN.PERSISTED.DB_KKF_MAIN.PERSISTED.GRUBHUB_ORDER_DAILY table
        # Transform and load the <DATA_SOURCE_NAME> <DATASET_NAME> data into the Snowflake DB_KKF_MAIN.PERSISTED.GRUBHUB_ORDER_DAILY table
        # Merge the <DATA_SOURCE_NAME> <DATASET_NAME> data into the Snowflake DB_KKF_MAIN.GRUBHUB.FACT_ORDER table
        # Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
####################################################################################################


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
) as dw_conn:


    ####################################################################################################
    # Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
    ####################################################################################################

    
    write_pandas(
        conn = dw_conn
        # Use df = df_subset, if you ARE using df_subset to remove specific columns from the S3 csv files before loading the data into Snowflake
        # ,df = df_subset
        # Use df = df_concat, if you are NOT using df_subset to remove specific columns from the S3 csv files before loading the data into Snowflake
        ,df = df
        ,table_name = "<Snowflake_Table_Name>"
        ,schema = "<Snowflake_Schema_Name>"
        # When set to False, no new table is created in Snowflake from the df_subset Pandas DataFrame
            # If set to True, a new table will be created in Snowflake from the df_subset Pandas DataFrame
        ,auto_create_table = False
        # When set to False, no quotes are added to each column value
            # When set to True, quotes are added to each column value
                # If the data already contains quotes, set to False
                    # Quotes can be added to column values if working with csv files, which ensures the csv data is not parsed incorrecty
        ,quote_identifiers = False
        # The default value is False,
        # which appends that data from the df_subset Pandas DataFrame to the end of the existing Snowflake table
            # When overwrite and auto_create_table are both set to True,
            # the Snowflake table is truncated before the data from the df_subset Pandas DataFrame is loaded into the Snowflake table
                # When overwrite is set to True and auto_create_table is set to False,
                # the Snowflake table is dropped and then recreated with the data from the df_subset Pandas DataFrame
        ,overwrite = True
    )
    

    ####################################################################################################
    # Establish a cursor
    ####################################################################################################


    # Setup a cursor in order to execute SQL queries to retrieve data from the Snowflake data warehouse
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
        # Assign the proper values for the following Snowflake object
            # Role
            # Warehouse
            # Database
        ####################################################################################################

        
        # Assume the <Snowflake_Role_Name> role
        cur.execute("""
            USE ROLE <Snowflake_Role_Name>;
        """)

        # Assume the <Snowflake_Warehouse_Name> ELT Production Warehouse
        cur.execute("""
            USE WAREHOUSE <Snowflake_Warehouse_Name>;
        """)

        # Assume the <Snowflake_Database_Name> database
        cur.execute("""
            USE DATABASE <Snowflake_Database_Name>;
        """)
        

        ####################################################################################################
        # Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
        ####################################################################################################

        
        # Verify whether or not the <SNOWFLAKE_TASK_NAME> task exists within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
            # Update the <SNOWFLAKE_TASK_NAME> task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake if it already exists
            # Insert the <SNOWFLAKE_TASK_NAME> task record into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake if it does NOT already exist
        cur.execute("""
            MERGE INTO DB_KKF_MAIN.AUTOMATION.TASK_LIST T
                
                USING
                (
                    -- Assign the necessary values for each column
                    SELECT
                        '<SNOWFLAKE_TASK_NAME>' AS TASK_NAME
                        ,'<SNOWFLAKE_TASK_DESCRIPTIONI>' AS TASK_DESCRIPTION
                        ,'<SNOWFLAKE_TASK_FREQUENCY>' AS TASK_FREQUENCY
                        ,'<SNOWFLAKE_TASK_DAY_OF_WEEK>' AS TASK_DAY_OF_WEEK
                        ,'<SNOWFLAKE_TASK_TIME_OF_DAY>' AS TASK_TIME_OF_DAY
                        ,NULL AS TASK_PREDECESSOR_NAME
                        ,CURRENT_DATE() AS TASK_LAST_RUN_START_DATE
                        ,TO_TIME(CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP())) AS TASK_LAST_RUN_START_TIME_IN_EST
                        ,NULL AS TASK_LAST_RUN_END_DATE
                        ,NULL AS TASK_LAST_RUN_END_TIME_IN_EST
                        ,NULL AS TASK_LAST_RUN_DURATION_IN_SECONDS
                ) S
                ON T.TASK_NAME = S.TASK_NAME
                
                -- Update the <SNOWFLAKE_TASK_NAME> task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
                -- Set the TASK_LAST_RUN_START_DATE & TASK_LAST_RUN_START_TIME_IN_EST with the current date and time in EST that the TASK started
                WHEN MATCHED
                    
                    THEN UPDATE SET
                        T.TASK_LAST_RUN_START_DATE = S.TASK_LAST_RUN_START_DATE
                        ,T.TASK_LAST_RUN_START_TIME_IN_EST = S.TASK_LAST_RUN_START_TIME_IN_EST
                
                -- Insert the <SNOWFLAKE_TASK_NAME> task record into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
                WHEN NOT MATCHED
                    
                    THEN INSERT VALUES
                    (
                        S.TASK_NAME
                        ,S.TASK_DESCRIPTION
                        ,S.TASK_FREQUENCY
                        ,S.TASK_DAY_OF_WEEK
                        ,S.TASK_TIME_OF_DAY
                        ,S.TASK_PREDECESSOR_NAME
                        ,S.TASK_LAST_RUN_START_DATE
                        ,S.TASK_LAST_RUN_START_TIME_IN_EST
                        ,S.TASK_LAST_RUN_END_DATE
                        ,S.TASK_LAST_RUN_END_TIME_IN_EST
                        ,S.TASK_LAST_RUN_DURATION_IN_SECONDS
                    )
            ;
        """)
        

        ####################################################################################################
        # Truncate the Snowflake DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        ####################################################################################################

        
        # Truncate tabe DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table so the data can be replace with updated data
        cur.execute("""
            TRUNCATE TABLE DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME>;
        """)
        

        ####################################################################################################
        # Transform and load the review data into the Snowflake DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        ####################################################################################################

        
        # Extract the raw data from the DB_KKF_MAIN.TRANSIENT.<TRANSIENT_TABLE_NAME> table
        # Transform the raw data to make it more useable for reporting purposes
        # Load the transformed data into the DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        cur.execute("""
            INSERT INTO DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME>

                SELECT DISTINCT
                    UPPER(TRIM(COLUMN_NAME_1)) AS COLUMN_NAME_1
                    ,TO_DATE(TRIM(COLUMN_NAME_2)) AS COLUMN_NAME_2
                    ,TO_NUMBER(TRIM(COLUMN_NAME_...N), 10, 2) AS COLUMN_NAME_...N

                FROM DB_KKF_MAIN.TRANSIENT.<TRANSIENT_TABLE_NAME>
                    
                WHERE <WHERE_CLAUSE_LOGIC>
            ;
        """)


        ####################################################################################################
        # Truncate the Snowflake DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
        ####################################################################################################

        
        # Truncate tabe DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table so the data can be replace with updated data
        cur.execute("""
            TRUNCATE TABLE DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME>;
        """)
        

        ####################################################################################################
        # Transform and load the review data into the Snowflake DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
        ####################################################################################################

        
        # Extract the raw data from the DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        # Transform the raw data to make it more useable for reporting purposes
        # Load the transformed data into the DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
        cur.execute("""
            INSERT INTO DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME>

                SELECT
                    UPPER(TRIM(COLUMN_NAME_1)) AS COLUMN_NAME_1
                    ,TO_DATE(TRIM(COLUMN_NAME_2)) AS COLUMN_NAME_2
                    ,TO_NUMBER(TRIM(COLUMN_NAME_...N), 10, 2) AS COLUMN_NAME_...N

                FROM DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME>
                    
                WHERE <WHERE_CLAUSE_LOGIC>
            ;
        """)


        ####################################################################################################
        # Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
        ####################################################################################################

        
        # Update the <SNOWFLAKE_TASK_NAME> task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
        # Set the TASK_LAST_RUN_END_DATE & TASK_LAST_RUN_END_TIME_IN_EST with the current date and time in EST that the TASK started
        cur.execute("""
            UPDATE DB_KKF_MAIN.AUTOMATION.TASK_LIST

                SET
                    TASK_LAST_RUN_END_DATE = CURRENT_DATE()
                    ,TASK_LAST_RUN_END_TIME_IN_EST = TO_TIME(CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP()))
                
                WHERE TASK_NAME = '<SNOWFLAKE_TASK_NAME>'
            ;
        """)
        
        '''
        # Update the <SNOWFLAKE_TASK_NAME> task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
        # Set the TASK_LAST_RUN_DURATION_IN_SECONDS updated value from taking the difference between the task start date and time and task end date and time
        cur.execute("""
            UPDATE DB_KKF_MAIN.AUTOMATION.TASK_LIST

                SET TASK_LAST_RUN_DURATION_IN_SECONDS = DATEDIFF
                (
                    SECOND
                    ,TASK_LAST_RUN_START_TIME_IN_EST
                    ,TASK_LAST_RUN_END_TIME_IN_EST
                )
                
                WHERE TASK_NAME = '<SNOWFLAKE_TASK_NAME>'
            ;
        """)
        '''
        
        # Load the updated <SNOWFLAKE_TASK_NAME> task record into the DB_KKF_MAIN.AUTOMATION.TASK_RUN_HISTORY table in Snowflake that keeps history of all task runs
        cur.execute("""
            INSERT INTO DB_KKF_MAIN.AUTOMATION.TASK_RUN_HISTORY

                SELECT *
                
                FROM DB_KKF_MAIN.AUTOMATION.TASK_LIST
                
                WHERE TASK_NAME = '<SNOWFLAKE_TASK_NAME>'
            ;
        """)

"""
# Display a message that informs the developer that the Python data pipeline has completed
    # Comment out once the entire data pipeline has completed successfully
print()
print("The <DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook has completed successfully")
print()
"""