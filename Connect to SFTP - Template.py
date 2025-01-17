"""
    The "<DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook" pulls <DATASET_NAME> data from <DATA_SOURCE_NAME> and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

    Data Pipeline Process:

        Step 1: Install Required Python Packages
        
        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to the SFTP

        Step 4: Establish a SFTP client object

        Step 5: Identify the SFTP file path file(s)

        Step 6: Connect to the SFTP file path file

        Step 7: Load the data from the SFTP file path file into a Pandas DataFrame

        Step 8: Convert the Pandas DataFrame to the proper format

        Step 9: ENTER THE DATA LOAD LOGIC HERE

        Step 10: Delete the SFTP file path file
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

# Install the paramiko Python package
    # Used to connect to SFTP environments	
%pip install paramiko

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


# Enables the ability to connect to the SFTP
    # pysftp not longer being maintained, therefore should use paramiko instead
import paramiko

# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
import pandas as pd


####################################################################################################
# Step 3: Setup the credentials to connect to the SFTP
    # Open a SSH connection and connect to the SFTP
        # SSH uses encryption to establish a secure connection between a client and a server, such as a SFTP server
####################################################################################################


# Establish the SSHClient in order to interact with the SFTP SSH server
ssh_client = paramiko.SSHClient()

# Set the policy for handling unknown host keys to autmoatically add the host keys to the known hosts file
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect to the SFTP SSH server
ssh_client.connect(
    hostname = "<Host_Name>"
    ,port = "<Port_Number>"
    ,username = "<Username>"
    ,password = "<Password>"
    ,look_for_keys = False
)

# Set the SFTP file path that contains the source file(s)
    # "." is the default file path used if no file path is specified
        # The "." default file path should be used if the file(s) do NOT have a file path / directory
            # i.e. the file(s) are not stored in a folder
sftp_file_path = "<Path_To_SFTP_File(s)>"


####################################################################################################
# Step 4: Establish a SFTP client object
####################################################################################################


# Establish a SFTP client object, which enables the ability to interact with the SFTP
    # The connection to the SFTP closes automatically after the with block is exited
        # Therefore, no need for the sftp.close() command
with ssh_client.open_sftp() as sftp:


    ####################################################################################################
    # Step 5: Identify the SFTP file path file(s)
        # Loop through the SFTP file path file(s)
    ####################################################################################################


    # Create a list of files that are contained within the SFTP file path, specified above
        # Loop through each of the files, one at a time, within the SFTP file path, specified above
            # This ensures that the same operations are performed on each file, one at a time
    for file in sftp.listdir(sftp_file_path):


        ####################################################################################################
        # Step 6: Connect to the SFTP file path file
            # Open the SFTP file path file
        ####################################################################################################


        # Open the SFTP file in order to interact with the file within the SFTP file path, specified above
            # The file is automatically closed after the with block is exited
                # Therefore no need for the remote_file.close() command
        with sftp.open(file) as sftp_file:
        

            ####################################################################################################
            # Step 7: Load the data from the SFTP file path file into a Pandas DataFrame
            ####################################################################################################


            # Prefetches the data within the SFTP file in the background as soon as the file is open
            # Prefetch helps speed up the process of pulling the data from the SFTP file and loading it to the df Pandas DataFrame
            sftp_file.prefetch()

            # Load the SFTP file into the a Pandas DataFrame
            df = pd.read_csv(sftp_file)

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
        # Step 8: Convert the Pandas DataFrame to the proper format
        ####################################################################################################
    

        # Convert the Pandas DataFrame to a CSV string in a new Pandas DataFrame
            # This enables the ability to write the data within the Pandas DataFrame into a Azure Blob Storage file
        df_string = df.to_csv(index = False)

        # Display the data within the new Pandas DataFrame
            # Remove once the data has been verified
        print(df_string)


        ####################################################################################################
        # Step 9: ENTER THE DATA LOAD LOGIC HERE
        ####################################################################################################


        # ENTER THE DATA LOAD LOGIC HERE FOR LOADING THE DATA INTO THE DESTINATION LOCATION


        ####################################################################################################
        # Step 10: Delete the SFTP file path file
        ####################################################################################################


        # Delete the SFTP file after it has been loaded into the destination, so that the file does not get loaded again
        sftp.remove(f"{sftp_file_path}/{file}")