"""
    The "<DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook" pulls <DATASET_NAME> data from <DATA_SOURCE_NAME> and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

    Data Pipeline Process:

        Step 1: Install Required Python Packages
        
        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to Azure Blob Storage

        Step 4: Establish a BlobServiceClient

        Step 5: Establish a ContainerClient to identify source files

        Step 6: Identify the Azure Blob Storage source container file(s)

        Step 7: Loop through the Azure Blob Storage source container file(s)

        Step 8: Establish a second ContainerClient

        Step 9: Establish a BlobClient

        Step 10: Load the data from the Azure Blob Storage source container file into a Pandas DataFrame

        Step 11: Convert the Pandas DataFrame to the proper format

        Step 12: ENTER THE DATA LOAD LOGIC HERE

        Step 13: Establish a third ContainerClient

        Step 14: Establish a second BlobClient

        Step 15: Load the data within the formatted Pandas DataFrame into the Azure Blob Storage archive container file

        Step 16: Delete the Azure Blob Storage source container
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
        # This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks
##############################################################################################################


# Install the azure-identity Python package
    # Used to connect to Azure Blob Storage	
%pip install azure-identity

# Install the azure-storage-blob Python package
    # Allows you to manipulate Azure Storage resources and blob containers	
%pip install azure-storage-blob

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


# Enables the ability to access and manipulate Azure Blob Storeage containers and blobs
from azure.storage.blob import BlobServiceClient

# Enables the ability to utilize DataFrames, which are 2-dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
import pandas as pd


####################################################################################################
# Step 3: Setup the credentials to connect to Azure Blob Storage
####################################################################################################


# Account Name of the Azure Blob Storage environment
account_name = "krispykrunchychicken"

# Account Key for the Azure Blob Storage environment
account_key = "S0cNomPEW8GbWW5192PcTtIH/j8n5nFyOrfMyIeHVafDR3iQ2KEHNFpbWqQCjwvq86xJ1nwcuVMO+AStbBv4Dg=="

# Connection String for the Azure Blob Storage environment, which is created from the Account Name and Account Key listed above
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

# Name of the Azure Blob Storage container that the file(s) are either:
# - Loaded into from the source, such as a SFTP or other application
# - Extracted from, to load into the:
#    - Data warehouse
#    - Azure Blob Storage archive container, for future use if needed
container_name = "<Container_Name>"

# Name of the Azure Blob Storage archive container that the file(s) will be archived into, for future use if needed
archive_container_name = "<Archive_Container_Name>"


####################################################################################################
# Step 4: Establish a BlobServiceClient
    # Connect to Azure Blob Storage
####################################################################################################


# Establish the BlobServiceClient in order to interact with Azure Blob Storage at the account level
    # The connection to the BlobServiceClient closes automatically after the with block is exited
            # Therefore, no need for the blob_service_client.close() command
with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:


    ####################################################################################################
    # Step 5: Establish a ContainerClient to identify source files
        # Connect to the Azure Blob Storage source container, specified above
    ####################################################################################################


    # Establish the ContainerClient in order to interact with the Azure Blob Storage source container, specified above
        # This enables the ability to list the file(s) located within the Azure Blob Storage source container, specified above
    with blob_service_client.get_container_client(container_name) as container_client_list:


        ####################################################################################################
        # Step 6: Identify the Azure Blob Storage source container file(s)
        ####################################################################################################


        # Create a list of files that are contained within the Azure Blob Storage source container, specified above
            # This list will enable the ability to loop through each file, one at a time, and perform the same operations on each file
        blob_list = container_client_list.list_blob_names()


    ####################################################################################################
    # Step 7: Loop through the Azure Blob Storage source container file(s)
    ####################################################################################################


    # Loop through each of the files, one at a time, within the Azure Blob Storage source container, specified above
        # This ensures that the same operations are performed on each file, one at a time
    for blob in blob_list:


        ####################################################################################################
        # Step 8: Establish a second ContainerClient
            # Connect to the Azure Blob Storage source container, specified above
        ####################################################################################################


        # Establish the ContainerClient in order to interact with the Azure Blob Storage source container, specified above
        with blob_service_client.get_container_client(container_name) as container_client:


            ####################################################################################################
            # Step 9: Establish a BlobClient
                # Connect to the Azure Blob Storage source container file
            ####################################################################################################


            # Establish the BlobClient in order to interact with the file within the Azure Blob Storage source container, specified above
            with container_client.get_blob_client(blob) as blob_client:
                

                ####################################################################################################
                # Step 10: Load the data from the Azure Blob Storage source container file into a Pandas DataFrame
                ####################################################################################################


                # Load the Azure Blob Storage source container file into a Pandas DataFrame
                df = pd.read_csv(blob_client.download_blob())

                # Verify if there is any data within the new Pandas DataFrame
                    # Remove once the data has been verified
                if not df.empty:

                    print('The Pandas DataFrame contains data')

                else:

                    print('The Pandas DataFrame is empty')

                # Display the data within the Pandas DataFrame
                    # Remove once the data has been verified
                print(df)


        ####################################################################################################
        # Step 11: Convert the Pandas DataFrame to the proper format
        ####################################################################################################
    

        # Convert the Pandas DataFrame to a CSV string in a new Pandas DataFrame
            # This enables the ability to write the data within the Pandas DataFrame into a Azure Blob Storage file
        df_csv = df_subset.to_csv(index = False)

        # Verify if there is any data within the df_csv Pandas DataFrame
            # Comment out once the data has been verified
        """
        if not df_csv.empty:

            print('The df_csv Pandas DataFrame contains data:')
            print()
            print(df_csv)

        else:

            print('The df_csv Pandas DataFrame is empty')
        """


        ####################################################################################################
        # Step 12: ENTER THE DATA LOAD LOGIC HERE
        ####################################################################################################


        # ENTER THE DATA LOAD LOGIC HERE FOR LOADING THE DATA INTO THE DESTINATION LOCATION


        ####################################################################################################
        # Step 13: Establish a third ContainerClient
            # Connect to the Azure Blob Storage archive container, specified above
        ####################################################################################################


        # Establish a second ContainerClient in order to interact with the Azure Blob Storage archive container, specified above
        with blob_service_client.get_container_client(archive_container_name) as archive_container_client:


            ####################################################################################################
            # Step 14: Establish a second BlobClient
                # Connect to the Azure Blob Storage archive container file
            ####################################################################################################


            # Establish a second BlobClient in order to interact with the archive file within the Azure Blob Storage archive container, specified above
            with archive_container_client.get_blob_client(blob) as archive_blob_client:
                

                ####################################################################################################
                # Step 15: Load the data within the formatted Pandas DataFrame into the Azure Blob Storage archive container file
                ####################################################################################################


                # Create a new file, or overwrite an existing file, within the Azure Blob Storage archive container
                    # Set overwrite = False, if you do not want to overwrite existing files that contain the same name as the file you are currently trying to create
                archive_blob_client.upload_blob(df_csv, overwrite = True)


        ####################################################################################################
        # Step 16: Delete the Azure Blob Storage source container
        ####################################################################################################


        # Delete the file from the Azure Blob Storage source container
            # This prevents the same file from being loaded into the destination multiple times, which will create duplicate data
        blob_client.delete_blob()