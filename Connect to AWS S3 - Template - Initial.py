"""
    The "<DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook" pulls KKC operator review data from <DATA_SOURCE_NAME> and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

    Data Pipeline Process:

        Step 1: Install Required Python Packages & Libraries

        Step 2: Install Required Python Packages & Libraries

        Step 3: Setup the credentials to connect to the AWS S3 Bucket

        Step 4: Connect to the AWS S3 Bucket

        Step 5: Collect a list of file names for all files within the AWS S3 Bucket

        Step 6: Extract and Load the data from the file within the AWS S3 Bucket

        Step 7: Consolidate all csv files in the all_date List into 1 Pandas Dataframe

        Step 8: Select the Columns to Keep from the df Pandas DataFrame and load the data into the df_subset Pandas DataFrame

        Step 9: Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse
            1. Setup the credentials to connect to the Snowflake data warehouse
            2. Connect to the Snowflake data warehouse
            3. Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
            4. Establish the cur cursor
                1. Assign the proper values for the following Snowflake objects:
                    1. Role
                    2. Warehouse
                    3. Database
                2. Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
                3. Truncate the Snowflake DB_KKF_MAIN.PERSISTED.DB_KKF_MAIN.PERSISTED.GRUBHUB_ORDER_DAILY table
                4. Transform and load the GrubHub Order data into the Snowflake DB_KKF_MAIN.PERSISTED.GRUBHUB_ORDER_DAILY table
                5. Merge the GrubHub Order data into the Snowflake DB_KKF_MAIN.GRUBHUB.FACT_ORDER table
                6. Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
        # This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks
##############################################################################################################


# Install the boto3 Python package
    # Used to connect to AWS services, like S3 Bucket	
%pip install boto3

# Install the datetime Python package
    # Used when working with and manipulating dates and times	
%pip install datetime

# Install the pandas Python package
    # Used to store data in Series and DataFrames	
%pip install pandas

# Install the snowflake-connector-python[pandas] Python package
    # Used to connect to Snowflake and utilize Pandas DataFrames	
%pip install snowflake-connector-python[pandas]

# Restart the kernel to use updated packages
    # Databricks specific command that is required to use any newly install Python packages listed above
%restart_python


##############################################################################################################
# Step 2: Install Required Python Libraries
    # Install all of the require Python libraries in order to perform the necessary actions within the Python notebook
##############################################################################################################


# Used when working with and manipulating dates and times
from datetime import datetime, timedelta

# Enables the ability to connect to multiple AWS services, such as S3 Bucket
import boto3

# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import pandas as pd

# Enables the ability to connect to the Snowflake Data Warehouse
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import snowflake.connector

# Enables the ability to write data into Snowflake from Pandas DataFrames
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
from snowflake.connector.pandas_tools import write_pandas


##############################################################################################################
# Step 3: Setup the credentials to connect to the AWS S3 Bucket
##############################################################################################################


# The access ID assigned to the account connecting to the AWS service
s3_access_id = "<AWS_S3_Access_ID>"

# The access key assigned to the account connecting to the AWS service
s3_secret_access_key = "<AWS_S3_Access_Key>"

# The AWS service that you would like to connect to
aws_service = "s3"

# The AWS region that the AWS service is set to
aws_region_name = "<AWS_Region>"

# The AWS S3 Bucket that you would like to connect to
s3_bucket = "<AWS_S3_Bucket>"

# The report associated with the file within the AWS S3 Bucket
    # Available datasets assigned to the account connecting to the AWS service
file_name = "<File_Name>"

# The extension of the file within the AWS S3 Bucket
s3_file_extension = "<File_Extension>"

# The list collect the name for all of the files within the S3 bucket
    # This will allow us to verify if the file that we would like to extract exists
        # If the file does not exist, the data pipeline will not try to extract the file
            # This will prevent the data pipeline from failing
s3_file_list = []

# The list will consolidate all S3 csv files
    # Consolidating all S3 csv files into a list, then converting the entire list into a DataFrame
    # prevents multiple DataFrames from being created in the for loop
        # This method is more effiecient and less computational than converting each dictionary into a DataFrame
        # and appending to one another, then consolidating all DataFrames into one
all_data = []

# The oldest date that you would like to pull data for
start_date = datetime(<4-Digit Year>, <1 or 2-digit Month>, <1 or 2-digit Day>)

# The date used to calulate the date variables below
today = datetime.now()

# Identifies how many times the for loop below will have to iterate for
    # 1 time per day that you would like to pull data for
days_passed = (today - start_date).days - <Number of day offset>


##############################################################################################################
# Step 4: Connect to the AWS S3 Bucket
##############################################################################################################


# Connect to the AWS S3 Bucket
    # The connection to the AWS S3 Bucket does not allow using the with function
        # The connection to the AWS S3 Bucket closes automatically and does not use the close() command
s3 = boto3.resource(
    service_name = aws_service
    ,region_name = aws_region_name
    ,aws_access_key_id = s3_access_id
    ,aws_secret_access_key = s3_secret_access_key
)


##############################################################################################################
# Step 5: Collect a list of file names for all files within the AWS S3 Bucket
##############################################################################################################


# Loop through each file within the AWS S3 Bucket
for s3_bucket_file in s3.Bucket(s3_bucket).objects.all():

    # Display all of the file names within the AWS S3 Bucket
        # Comment Out once the files have been verified
    # print()
    # print(s3_bucket_file.key)

    # Collect the name of each file within the AWS S3 Bucket
    s3_file_list.append(s3_bucket_file.key)

"""
# Display all of the files within the AWS S3 Bucket
    # Comment Out once the files have been verified
print()
print("AWS S3 Bucket Files:")
print()
print(s3_file_list)
"""


##############################################################################################################
# Step 6: Extract and Load the data from the file within the AWS S3 Bucket
##############################################################################################################


# Loop through the S3 bucket, once per day that we would like to pull data for
for i in range(days_passed) and today >= datetime.strptime("<Oldest Date you want data for>", "%Y-%m-%d"):

    # The date that the GET request is being executed
        # The date the data pipeline is executed
    file_date = today.strftime("%Y%m%d")

    # The year part of the file name within the AWS S3 Bucket
    s3_file_year = (today - timedelta(days = 3)).strftime("%Y")

    # The month part of the file name within the AWS S3 Bucket
    s3_file_month = (today - timedelta(days = 3)).strftime("%m")

    # The day part of the file name within the AWS S3 Bucket
    s3_file_day = (today - timedelta(days = 3)).strftime("%d")

    # The file within the AWS S3 Bucket that you would like to extract
    s3_file_name = f"{file_name}.{s3_file_extension}"

    # Verify if the desired file name exists within the AWS S3 Bucket
        # If the file exists within the AWS S3 Bucket, extaract the file
    if s3_file_name in s3_file_list:

        # Extract the data from the file within the AWS S3 Bucket
        s3_file = s3.Bucket(s3_bucket).Object(s3_file_name).get()

        # Load the data from s3_file into the df Pandas DataFrame
        df = pd.read_csv(s3_file['Body'], index_col = 0)

        """
        # Verify if there is any data within the df Pandas DataFrame
            # Comment out once the data has been verified
        if not df.empty:

            print("The df Pandas DataFrame contains data:")
            print()
            print(df)

        else:

            print("The df Pandas DataFrame is empty")
        """
    
        # Append each S3 csv file into the all_data list to consolidate all of the S3 csv files that are being extracted from S3
        all_data.append(df)

    # Adjust the date value in the today variable to subtract 1 day from the variable
        # This allowas us to look at and extract the S3 csv file for the next previous day
            # 1 day will be subtracted from the today variable after each S3 csv file is extracted until the end of the for loop
                # This allows us to extract the S3 csv file for each day from the date set in the start_date variable until the current date - 3 days
    today = today - timedelta(days = 1)


##############################################################################################################
# Step 7: Consolidate all csv files in the all_date List into 1 Pandas Dataframe
##############################################################################################################


# Concat all of the separate S3 csv files into a formal Pandas DataFrame
df_concat = pd.concat(all_data)

"""
# Verify if there is any data within the all_data Pandas DataFrame
    # Comment out once the data has been verified
print()
print("The df_concat Pandas DataFrame contains data:")
print()
print(df_concat)
"""


##############################################################################################################
# Step 8: Select the Columns to Keep from the df Pandas DataFrame and load the data into the df_subset Pandas DataFrame
##############################################################################################################


"""
# Display the columns within the df_concat Pandas DataFrame
    # This will allow you to identify the columns within the dataset
        # This allows you to be able to remove any unecessary columns, using df_subset below, from the dataset before loading into Snowflake
print()
print("Here are all of the columns within the df_concat Pandas DataFrame")
print()
print(df_concat.columns)
"""

# Verify if the df_subset Pandas DataFrame contains data
# Verify if the df_concat Pandas DataFrame contains data
    # If the df_subset Pandas DataFrame contains data, load the data into Snowflake
#if not df_subset.empty:
if not df_concat.empty:
    """
    # Select the desired columns to keep from the df Pandas DataFrame
        # This method is used to remove any unwanted data that was extracted from the file within the AWS S3 Bucket
            # This is similar to the SELECT clause in SQL
    df_subset = df_concat[
        [
            '<Column_1>'
            ',<Column_2>'
            ,'<Column_...N>'
        ]
    ]
    """

    """
    # Verify if there is any data within the df_subset Pandas DataFrame
        # Comment out once the data has been verified
    if not df_subset.empty:

        print('The df_subset Pandas DataFrame contains data:')
        print()
        print(df_subset)

    else:

        print('The df_subset Pandas DataFrame is empty')
    """


    ####################################################################################################
    # Step 9: Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse
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
            ,df = df_concat
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
            # Truncate the Snowflake DB_KKF_MAIN.PERSISTED.DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
            ####################################################################################################

            
            # Truncate tabe DB_KKF_MAIN.PERSISTED.DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table so the data can be replace with updated data
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