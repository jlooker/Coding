"""
    The "<DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook" pulls <DATASET_NAME> data from <DATA_SOURCE_NAME> and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

    Data Pipeline Process:

        Step 1: Install Required Python Packages
        
        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to the REST API

        Step 4: Connect to the Rest API Authenticator URL (Optional)
            1. Collect the access token
            2. Combined the token type with the access token

        Step 5: Connect to the Rest API endpoint and extract the data from the 1st page
            1. Connect to the 1st page of the Rest API enpoint
                1. Extract the data from the 1st page of the Rest API endpoint into the response_data variable

        Step 6: Load the data from the Rest API endpoint into the all_data list defined at the beginning of the notebook

        Step 7: Loop through the remaining pages within the Rest API endpoint and extract the remaining data within each page (Optional)
            1. Connect to the next page within the Rest API endpoint
                1. Extract the data from the next page of the Rest API endpoint into the response_data variable
            2. Load the data from the response_data variable into the all_data list that we defined in step 2
            3. Collect and overwrite the <NEXT_PAGE_TOKEN_NAME> value, if it exists, into the next_page_token variable

        Step 8: Load the data from the all_data list into the df Pandas DataFrame

        Step 9: Select the Columns to Keep from the df Pandas DataFrame and load the data into the df_subset Pandas DataFrame

        Step 10: Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse
            1. Setup the credentials to connect to the Snowflake data warehouse
            2. Connect to the Snowflake data warehouse
            3. Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
            4. Establish the cur cursor
                1. Assign the proper values for the following Snowflake objects:
                    1. Role
                    2. Warehouse
                    3. Database
                2. Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
                3. Truncate the Snowflake DB_KKF_MAIN.PERSISTED.DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
                4. Transform and load the <DATA_SOURCE_NAME> <DATASET_NAME> data into the Snowflake DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
                5. Merge the <DATA_SOURCE_NAME> <DATASET_NAME> data into the Snowflake DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
                6. Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
        # This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks
##############################################################################################################


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


####################################################################################################
# Step 2: Install Required Python Libraries
    # Install all of the require Python libraries in order to perform the necessary actions within the Python notebook
####################################################################################################


# Used when working with and manipulating dates and times
from datetime import datetime, timedelta

# Enables the ability to connect to a Rest API
import requests

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


####################################################################################################
# Step 3: Setup the credentials to connect to the Rest API
    # Define all of the necessary variables that will be used to connect to the Rest API endpoint
        # Below are some samples of the variables that may be needed to connect to the Rest API endpoint
####################################################################################################


# The base URL of the Rest API
host_name = "<BASE_URL>"

# The API Key assigned to the account being connected to via the Rest API
api_key = "<API_KEY>"

# The credentials that will be used to connect to the REST API authentication URL
    # Each Rest API environment requires different vairables
auth_data = {
    # The client id acts as a username to access the REST API authentication URL
        # You may need to update the Client ID key name based on the Rest API that you are trying to connect to
    'clientId': "<Client_ID>"
    # The client secret acts as a password to acces the REST API authentication URL
        # You may need to update the Client Secret key name based on the Rest API that you are trying to connect to
    ,'clientSecret': "<Client_Secret>"
    # The access type required to access the REST API authentication URL
        # Can be either of below: (userAccessType and grant_type)
            # Remove all except for the one required by the REST API
        # You may need to update the User Access Type key name based on the Rest API that you are trying to connect to
    ,'userAccessType': "<Access_Type>"
    # You may need to update the Grant Type key name based on the Rest API that you are trying to connect to
    ,'grant_type': "client_credentials"
}

# The authentication URL/path that is added to the base URL
    # The authentication path enables the ability to authenticte the connection to the REST API and receive an access token
        # The access token is required in order to gain access to the redirect URL, which contains that data we want to export
auth_url = "/authentication/v1/authentication/login"

# The date used to calulate the date variables below
today = datetime.now()

# The date that the GET request is being executed
    # The date the data pipeline is executed
version_date = today.strftime("%Y%m%d")

# The date that identifies the start of the date range used to pull records within a specific date range
    #  The date should be the Sunday of the week prior to the week the data pipeline is executed
start_date = (today - timedelta(days = 9)).strftime("%Y-%m-%d")

# The date that identifies the end of the date range used to pull records within a specific date range
    # The date should be the Sunday of the week prior to the week the data pipeline is executed
end_date = (today - timedelta(days = 3)).strftime("%Y-%m-%d")

# The redirect path that is added to the base URL
    # An access token ir required to gain access to this URL, which is obtained by using the authentication URL
        # The redirect URL is where the data is located that we want to extract
redirect_url = f"<ENDPOINT_URL>{<ENDPOINT_URL_VARIABLE>}"

# The list that will consolidate each dictionary from all of the Rest API endpoint pages
    # Consolidating all dictionaries into a list, then converting the entire list into a DataFrame
    # prevents multiple DataFrames from being created in the while loop
        # This method is more effiecient and less computational than converting each dictionary into a DataFrame
        # and appending to one another, then consolidating all DataFrames into one
all_data = []


####################################################################################################
# Step 4: Connect to the Rest API Authenticator URL (Optional)
    # May not need to connect to the Rest API Authenticator URL
        # Remove if not needed
####################################################################################################


# Connect to the Rest API
    # Send a POST request to be assigned an access token
        # The connection to the Rest API closes automatically after the with block is exited
            # Therefore, no need for the token_response.close() command
with requests.post(
    # Combine the host_name (base URL) with the auth_rul (authentication URL)
        # This creates the full authenticaion URL to connect to to get the access token
    url = f"{host_name}{auth_url}"
    # Convert the login credentials to JSON format
    ,json = auth_data
) as token_response:
    
    # Use the json decoder to collect and read the json data from the token_response Rest API endpoint connection
        # Comment Out once the connection has been verified
    token_response_json = token_response.json()

    # Display the connection credentials used in the token_response POST request
        # Comment Out once the connection has been verified
    print()
    print(f"token_response_json: {token_response_json}")
    print()
    

    ####################################################################################################
    # Test the connection to the Rest API Authenticator URL
        # Comment Out once the connection has been verified
    ####################################################################################################


    # Display the connection status code to the Rest API
        # Comment Out once the connection has been verified
    if token_response.status_code == 200:

        # # Display that the connection was successful along with the status code 200
            # Status code 200 means that the connection to the Rest API was successful
        print()
        print(f"The request to the Rest API authentication URL is successful with status code: {token_response.status_code}!")
        print()

        # print the JSON content from the POST request
            # Remove once the data has been verified
        #print(token_response.json())

        # print the JSON content from the token key/section of the POST request
            # Remove once the inner data from the token section has been verified
        #print(token_response.json()["<TOKEN_DICTIONARY/LIST_NAME>"])

        # print the access token from the JSON content from the token key/section of the POST request
            # Remove once the access token has been verified
        #print(token_response.json()["<TOKEN_DICTIONARY/LIST_NAME>"].get("<ACCESS_TOKEN_KEY_NAME>"))

    else:

        # Display that the connection failed along with the status code
            # The status code can be used to help identify possible issues with the connection
        print()
        print(f"The request to the Rest API authentication URL failed with status code: {token_response.status_code}")
        print()


    ####################################################################################################
    # Collect the access token
    ####################################################################################################

    
    # Use the json decoder to collect and read the json data from the token_response Rest API endpoint connection
    access_token = token_response.json()["<TOKEN_DICTIONARY/LIST_NAME>"].get("<ACCESS_TOKEN_KEY_NAME>")

    # Display the access token from the POST request
        # Comment Out once the access token has been verified
    print(f"Access Token: {access_token}")


    ####################################################################################################
    # Combined the token type with the access token
    ####################################################################################################


    # Each Rest API environment requires different vairables
        # Combine the token type with the access token
            # The combined values are used as secret/key/password to connect to the redirect URL
                # Remove the External ID section if no GUID is required to connect to the Rest API Redirect URL
    redirect_auth = {"Authorization": "Bearer " + access_token}

    # Display the authorizaion value
        # Comment Out once the authorizaion value has been verified
    print(f"Redirect Auth: {redirect_auth}")


####################################################################################################
# Step 5: Connect to the Rest API endpoint and extract the data from the 1st page
    # Connect to the 1st page of the Rest API enpoint
        # Extract the data from the 1st page of the Rest API endpoint into the response_data variable
####################################################################################################


# Connect to the 1st page of the Rest API enpoint
    # Send a GET request that will establish the connection to the 1st page of the Rest API endpoint
        # The connection to the Rest API closes automatically after the with block is exited
            # Therefore, no need for the initial_data_response.close() command
with requests.get(
    # Combine the host_name (base URL) with the redirect URL (endpoint extension)
        # This creates the full Rest API endpoint URL to connect to the desired dataset on the 1st page within the Rest API
    url = f"{host_name}{redirect_url}"
    # Convert the login credentials to JSON format
        # Will NOT need to connect to the Rest API Authenticator URL if no access toekn is needed
            # Remove if not needed
    ,headers = redirect_auth
) as initial_data_response:


    ##############################################################################################################
    # Test the connection to the Rest API endpoint
        # Comment out once the connection has been verified
    ##############################################################################################################

    
    
    # Display the connection status code to the Rest API endpoint
        # Comment out once the connection has been verified
    if initial_data_response.status_code == 200:

        # Display that the connection was successful along with the status code 200
            # Status code 200 means that the connection to the Rest API was successful
        print()
        print(f"The initial request to the Rest API endpoint is successful with status code: {initial_data_response.status_code}!")

    else:

        # Display that the connection failed along with the status code
            # The status code can be used to help identify possible issues with the connection
        print()
        print(f"The initial request to the Rest API endpoint failed with status code: {initial_data_response.status_code}")
    

    
    ##############################################################################################################
    # Extract the data from the 1st page of the Rest API endpoint into the response_data variable
    ##############################################################################################################


    # Use the json decoder to collect and read the json data from the initial_data_response Rest API endpoint connection
    response_data = initial_data_response.json()


##############################################################################################################
# Step 6: Load the data from the Rest API endpoint into the all_data list defined at the beginning of the notebook
##############################################################################################################


# Load the data from the response_data variable into the all_data list that we defined in step 2
all_data.extend(response_data["<JSON_DICTIONARY/LIST_NAME>"]["<JSON_DICTIONARY/LIST_NAME>"])

# Collect and save the <NEXT_PAGE_TOKEN_KEY_NAME> value into the next_page_token variable
next_page_token = response_data["<JSON_DICTIONARY/LIST_NAME>"]["<NEXT_PAGE_TOKEN_KEY_NAME>"]


####################################################################################################
# Step 7: Loop through the remaining pages within the Rest API endpoint and extract the remaining data within each page (Optional)
    # Connect to the next page within the Rest API endpoint
        # Extract the data from the next page of the Rest API endpoint into the response_data variable
    # Load the data from the response_data variable into the all_data list that we defined in step 2
    # Collect and overwrite the <NEXT_PAGE_TOKEN_KEY_NAME> value, if it exists, into the next_page_token variable
####################################################################################################


# Loop through each page within the Rest API endpoint connection using the <NEXT_PAGE_TOKEN_KEY_NAME> to identify the next page
    # Once there is no <NEXT_PAGE_TOKEN_KEY_NAME>, the loop will end
while next_page_token:


    ##############################################################################################################
    # Connect to the next page within the Rest API endpoint
    ##############################################################################################################


    # Send a GET request that will establish the connection to the next page of the Rest API endpoint
        # The connection to the Rest API closes automatically after the with block is exited
            # Therefore, no need for the additional_data_response.close() command
    with requests.get(
        # Combine the host_name (base URL) with the redirect URL (endpoint extension) and <NEXT_PAGE_TOKEN_KEY_NAME>
            # This creates the full endpoint URL to connect to the desired dataset on the next page within the Rest API
        url = f"{host_name}{redirect_url}{next_page_token}"
    ) as additional_data_response:
 
    
        ##############################################################################################################
        # Test the connection to the Rest API endpoint
            # Comment out once the connection has been verified
        ##############################################################################################################

        
        """
        # Display the connection status code to the Rest API endpoint
            # Comment out once the connection has been verified
        if initial_data_response.status_code == 200:
    
            # Display that the connection was successful along with the status code 200
                # Status code 200 means that the connection to the Rest API was successful
            print()
            print(f"The additional request to the Rest API endpoint is successful with status code: {initial_data_response.status_code}!")
    
        else:
    
            # Display that the connection failed along with the status code
                # The status code can be used to help identify possible issues with the connection
            print()
            print(f"The additional request to the Rest API endpoint failed with status code: {initial_data_response.status_code}")
        """

        
        ##############################################################################################################
        # Extract the data from the next page of the Rest API endpoint into the response_data variable
        ##############################################################################################################
    
    
        # Use the json decoder to collect and read the json data from the initial_data_response Rest API endpoint connection
        response_data = additional_data_response.json()

    
    ##############################################################################################################
    # Load the data from the response_data variable into the all_data list that we defined in step 2
    ##############################################################################################################

    # Consolidate the json data from each page into a single list
        # Only pull the json data located under the docs list, located under the response dictionary
    all_data.extend(response_data["<JSON_DICTIONARY/LIST_NAME>"]["<JSON_DICTIONARY/LIST_NAME>"])


    ##############################################################################################################
    # Collect and overwrite the <NEXT_PAGE_TOKEN_KEY_NAME> value, if it exists, into the next_page_token variable
    ##############################################################################################################
    

    # Verify if the current page has the <NEXT_PAGE_TOKEN_KEY_NAME> key value pair
        # If the current page contains the <NEXT_PAGE_TOKEN_KEY_NAME> key value pair, collect the <NEXT_PAGE_TOKEN_KEY_NAME> value
            # The <NEXT_PAGE_TOKEN_KEY_NAME> key value pair is located under the response dictionary
    if '<NEXT_PAGE_TOKEN_KEY_NAME>' in response_data["<JSON_DICTIONARY/LIST_NAME>"]:
        
        # collect and overwrite the <NEXT_PAGE_TOKEN_KEY_NAME> value that will be used to access the next page within the Rest API endpoint
            # The <NEXT_PAGE_TOKEN_KEY_NAME> key value pair is located under the response dictionary
        next_page_token = response_data["<JSON_DICTIONARY/LIST_NAME>"]["<NEXT_PAGE_TOKEN_KEY_NAME>"]

        # Display the <NEXT_PAGE_TOKEN_KEY_NAME> value
            # Comment out once the <NEXT_PAGE_TOKEN_KEY_NAME> value has been verified
        # print()
        # print("The <NEXT_PAGE_TOKEN_KEY_NAME> field is: {next_page_token}")

    # If the current page does NOT contain the <NEXT_PAGE_TOKEN_KEY_NAME> key value pair, exit the loop
    else:

        # Display that the <NEXT_PAGE_TOKEN_KEY_NAME> value is missing, signifying that you are currently on the last page
            # Comment out once the <NEXT_PAGE_TOKEN_KEY_NAME> value has been verified
        # print()
        # print("The <NEXT_PAGE_TOKEN_KEY_NAME> field is missing on this page")
        # print()
        # print("This is the last page!")
        
        # Exit the while loop since there are no more pages after the current page to collect data from
        break

# Display that the while loop has been closed
    # Comment out once the while loop closure has been verified
#print()
#print("The while loop is done")


####################################################################################################
# Step 8: Load the data from the all_data list into the df Pandas DataFrame
####################################################################################################


# Load the data from the all_data list into a Pandas DataFrame
    # pd.json_normalize is used when the json data contains multiple nested lists
        # pandas.DataFrame() can be used for more simple json data that does NOT contains multiple nested lists
df = pd.json_normalize(
    # The list that contains all of the consolidated data from each of the Rest API endpoint pages
    all_data
    # record path is used to identify nested lists that need further parsing
        # Can contain list of nested element lists
    #record_path = ""
    # meta is used to identify all elements than do not need further parsing
        # Can contain list of elements
    #,meta = ""
    # prefix attached to all elements that do not need further parsing
    #,meta_prefix = ""
    # prefix attached to all elements that need further parsing
    #,record_prefix = ""
    # sep is used to separated parent and child key names
        # Defult is "."
    ,sep = "_"
)

# Verify if there is any data within the df Pandas DataFrame
    # Comment out once the data has been verified
"""
if not df.empty:

    print('The df Pandas DataFrame contains data:')
    print()
    print(df)

else:

    print('The df Pandas DataFrame is empty')
"""


####################################################################################################
# Step 9: Select the Columns to Keep from the df Pandas DataFrame and load the data into the df_subset Pandas DataFrame
####################################################################################################


# Select the desired columns to keep from the df Pandas DataFrame
    # This method is used to remove any unwanted data that was extracted from the Rest API endpoint
        # This is similar to the SELECT clause in SQL
df_subset = df[
    [
        'COLUMN_NAME_1'
        ,'COLUMN_NAME_2'
        ,'COLUMN_NAME_...N'
    ]
]

# Verify if there is any data within the df_subset Pandas DataFrame
    # Comment out once the data has been verified
"""
if not df_subset.empty:

    print('The df_subset Pandas DataFrame contains data:')
    print()
    print(df_subset)

else:

    print('The df_subset Pandas DataFrame is empty')
"""


####################################################################################################
# Step 10: Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse
    # Setup the credentials to connect to the Snowflake data warehouse
    # Connect to the Snowflake data warehouse
    # Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
    # Establish the cur cursor
        # Assign the proper values for the following Snowflake objects:
            # Role
            # Warehouse
            # Database
        # Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
        # Truncate the Snowflake DB_KKF_MAIN.PERSISTED.DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        # Transform and load the <DATA_SOURCE_NAME> <DATASET_NAME> data into the Snowflake DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        # Merge the <DATA_SOURCE_NAME> <DATASET_NAME> data into the Snowflake DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
        # Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
####################################################################################################


# The connection to the Snowflake data warehouse closes automatically after the with block is exited
    # Therefore, no need for the dw_conn.close() command
with snowflake.connector.connect(
    # The username should be tied to a service account, rather than and specific individual
    user = "ELTADMIN"
    # The password should be tied to a service account, rather than and specific individual
    ,password = "*Easy_ELT_123!*"
    # The account consists of 3 parts separated by a decimal (".")
        # Part 1: Snowflake account identifier
        # Part 2: Snowflake cloud region
        # Part 3: Snowflake cloud provider
    ,account = "xl70960.east-us-2.azure"
    # The Snowflake warehouse that will be used to write the data into Snowflake
    ,warehouse = "<DATA_WAREHOUSE_WAREHOUSE_NAME>"
    # The Snowflake database where the data will be written into
    ,database = "DB_KKF_MAIN"
) as dw_conn:
    

    ####################################################################################################
    # Load the data from the df_subset Pandas DataFrame into the Snowflake data warehouse table
    ####################################################################################################

    
    write_pandas(
        conn = dw_conn
        ,df = df_subset
        ,table_name = "<TRANSIENT_TABLE_NAME>"
        ,schema = "TRANSIENT"
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


        # Assume the SYSADMIN role
        cur.execute("""
            USE ROLE SYSADMIN;
        """)

        # Assume the <DATA_WAREHOUSE_WAREHOUSE_NAME> ELT Production Warehouse
        cur.execute("""
            USE WAREHOUSE <DATA_WAREHOUSE_WAREHOUSE_NAME>;
        """)

        # Assume the DB_KKF_MAIN database
        cur.execute("""
            USE DATABASE DB_KKF_MAIN;
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
        # Merge the review data into the Snowflake DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
            # The merge statement ensures that no duplicate records get loaded into the DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
            # as this will skew any reports that utilize the <DATA_SOURCE_NAME> <DATASET_NAME>
        ####################################################################################################


        # Extract the transformed data from the DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME> table
        # Load the transformed data into the DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table
        cur.execute("""
            MERGE INTO DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> T
                
                USING
                (
                    SELECT DISTINCT
                        COLUMN_NAME_1
                        ,COLUMN_NAME_2
                        ,COLUMN_NAME_...N
                    
                    FROM DB_KKF_MAIN.PERSISTED.<PERSISTED_TABLE_NAME>
                ) S
                ON T.COLUMN_NAME_1 = S.COLUMN_NAME_1
                AND T.COLUMN_NAME_2 = S.COLUMN_NAME_2
                AND T.COLUMN_NAME_...N = S.COLUMN_NAME_...N

                -- Insert new <Persisted_Table> records into the DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table in Snowflake
                WHEN NOT MATCHED
                
                    THEN INSERT VALUES
                    (
                        S.COLUMN_NAME_1
                        ,S.COLUMN_NAME_2
                        ,S.COLUMN_NAME_...N
                    )
                
                -- Update existing <Persisted_Table> records in the DB_KKF_MAIN.<PRESENTATION_SCHEMA_NAME>.<FACT/DIM_TABLE_NAME> table in Snowflake
                WHEN MATCHED
                    -- AND      -- AND is optional to add additional logic to Match statement when multiple Match statements are needed to perform different actions based on criteria
                                -- AND look at T. columns and S. columns, even comparing one to another in the desired order
                
                    THEN UPDATE

                        SET
                            T.COLUMN_NAME_1 = S.COLUMN_NAME_1
                            ,T.COLUMN_NAME_2 = S.COLUMN_NAME_2
                            ,T.COLUMN_NAME_...N = S.COLUMN_NAME_...N
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


# Display a message that informs the developer that the Python data pipeline has completed
    # Comment out once the entire data pipeline has completed successfully
print()
print("The <DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook has completed successfully")
print()