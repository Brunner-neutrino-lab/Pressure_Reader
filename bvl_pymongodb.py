# Module for the BvL MongoDB functions
# Authors: T. Totev, C. Chambers

# Importing the dependencies
from logging import error
import os
import json
import pandas
import csv
import sshtunnel
import paramiko
import pymongo
import datetime
import config as cfg
import numpy as np
import matplotlib.pyplot as plt
from Deprecated_code import PyMongoDBSnippet
from scipy import stats as st


def bvl_mongo_help(error_code='Infinity'):
    """
        Function returning a description of the error code returned by one of the bvl_pymongodb module functions.
        Attempting to pass a non-existant key will simply return the full dictionary of possible error codes.


        Parameters
        ----------
            error_code : int
                Integer error code whose description is to be returned

        Returns
        -------
            error_dict : dict
                dict with key:value pairs corresponding to all the possible error codes that can arise from
                functions in the module

            OR

            error_dict[str(error_code)] : str
                String describing the error corresponding to the numerical error code received
    """
    error_dict = {
        '0': 'Successful (no errors)',
        '1': 'Private key not found',
        '2': 'SSH connection failed',
        '3': 'Failed to create mongodb client or db_name/col_name issues',
        '4': 'CSV File does not exist',
        '5': 'Invalid CSV File contents, please check that it actually has data',
        '6': 'MongoDB write operation failed, no documents added to the database',
        '7': 'MongoDB query failure',
        '8': 'Conversion to pandas dataframe or save as csv failure',
        '9': 'Csv file already exists',
        '10': 'Incorrect password for database/collection deletion',
        '11': 'Custom file upload to Slowcontrol aborted',
        '12': 'Collection does not exist',
        '13': 'Empty Dataframe Returned by MongoDB',
        '14': 'Standard Deviation Cuts file not found',
        '15': 'Empty Standard Deviation Cuts file'
    }
    # Checking if the provided integer is in the list of error codes
    if not (str(error_code) in error_dict.keys()):
        return error_dict
    else:
        return error_dict[str(error_code)]


def load_std_cuts(cuts_file=cfg.STD_CUT_PATH):
    """
        Function that takes a file path and returns a formatted dictionary in format:
        Sensor_name1 : std_cut1
        Sensor_name2 : std_cut2
        Sensor_name3 : std_cut3
        Sensor_name4 : std_cut4
        .
        .
        .
        Parameters
        ----------
            cuts_file : string
                Path to the file containing the std cuts that are to be loaded

        Returns
        -------
            std_dict : dict
                dict with key:value pairs corresponding to all the std cuts for each sensor name in the file
            err_code: int
                Code for any errors that might have occured, 0 signifies success.
    """
    try:
        # Reading the txt file containing the std cuts into a dataframe
        std_df = pandas.read_csv(cuts_file, " ")

        # Formatting the dataframe into a dictionary
        std_dict = {std_df.iloc[i, 0]: float(
            std_df.iloc[i, 1]) for i in range(len(std_df))}

        return std_dict, 0

    except FileNotFoundError:
        return None, 14
    except pandas.errors.EmptyDataError:
        return None, 15


def ssh_connect(pkey_path=cfg.PKEY_FILE_PATH):
    """
        Creates an SSHTunnelForwarder object with an open SSH connection to the BvL MongoDB Instance. 
        The "server" variable returned from this function can be used to start and stop SSH connections using "server.start()"
        and "server.stop()". 

        Use 'client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)' to create a MongoClient after starting the
        SSH connection.

        Parameters
        ----------
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 

        Returns
        -------
        server: sshtunnel.SSHTunnelForwarder
            object used to open and close SSH connection to the specified host over a forwarded port
        err_code: int
            Code for any errors that might have occured, 0 signifies success.

    """
    try:
        key = paramiko.RSAKey.from_private_key_file(
            pkey_path)  # extracting private key
    except:
        return None, 1
    try:
        server = sshtunnel.SSHTunnelForwarder(  # creating an SSH tunnel forwarder
            cfg.MONGO_HOST,  # setting the host adress
            ssh_username=cfg.MONGO_USER,  # setting the ssh login-user
            ssh_pkey=key,  # setting the private key
            # setting MongoDB instance bind adress
            remote_bind_address=('127.0.0.1', 27017)
        )
        server.start()  # Starting the ssh connection
        return server, 0
    except:
        return None, 2


def delete_database(db_name, pkey_path=cfg.PKEY_FILE_PATH):
    """
        Function that deletes the database 'db_name' from the BvL MongoDB Instance
        If the name is 'tutorial' a password is not required. For deletion
        of any other database the user will be prompted to enter a password.

        ------USE WITH CAUTION------
        Parameters
        ----------
        db_name: str
            Name of the database to be deleted
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 

        Returns
        -------
        err_code: int
            Code for any errors that might have occured, 0 signifies success.
    """
    # If the user wants to delete a db other than 'tutorial'
    if not(db_name == 'tutorial'):
        # Prompting the user for password
        pw = input('Enter the correct password to procede:')
        if(pw == PyMongoDBSnippet.db_password):
            # If correct password attempting to establish ssh connection
            server, err_code = ssh_connect(pkey_path)
            if not(err_code == 0):
                return err_code
            else:
                try:
                    # Creating a MongoClient Object
                    # server.local_bind_port is assigned local port
                    client = pymongo.MongoClient(
                        '127.0.0.1', server.local_bind_port)

                    # Deleting the database
                    client.drop_database(db_name)

                    server.stop()
                    return err_code
                except:
                    err_code = 3
                    server.stop()
                    return err_code
        else:
            err_code = 7
            return err_code
    # If the user wants to delete 'tutorial'
    else:
        server, err_code = ssh_connect(pkey_path)
        if not(err_code == 0):
            return err_code
        else:
            try:
                # Creating a MongoClient Object
                # server.local_bind_port is assigned local port
                client = pymongo.MongoClient(
                    '127.0.0.1', server.local_bind_port)

                client.drop_database(db_name)

                server.stop()
                return err_code
            except:
                err_code = 3
                server.stop()
                return err_code


def delete_collection(db_name, col_name, pkey_path=cfg.PKEY_FILE_PATH):
    """
        Function that drops the collection 'col_name' from database 'db_name'.
        If the name is 'Slowcontrol' a password is required. For deletion
        of any other collection the user will not be prompted to enter a password.

        ------USE WITH CAUTION------
        Parameters
        ----------
        db_name: str
            Name of the database containing the collection to be deleted
        col_name: str
            name of the collection to be deleted
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 

        Returns
        -------
        err_code: int
            Code for any errors that might have occured, 0 signifies success.
    """
    # If the user is trying to delete 'Slowcontrol'
    if (col_name == 'Slowcontrol'):
        # Prompt user for password entry
        pw = input('Enter the correct password to procede:')
        if(pw == PyMongoDBSnippet.db_password):
            # Establish ssh connection
            server, err_code = ssh_connect(pkey_path)
            if not(err_code == 0):
                return err_code
            else:
                try:
                    # Creating a MongoClient Object
                    # server.local_bind_port is assigned local port
                    client = pymongo.MongoClient(
                        '127.0.0.1', server.local_bind_port)
                    db = client[db_name]
                    db.drop_collection(col_name)

                    server.stop()
                    return err_code
                except:
                    err_code = 3
                    server.stop()
                    return err_code
        else:
            err_code = 7
            return err_code
    # If the user is attempting to delete another database
    else:
        server, err_code = ssh_connect(pkey_path)
        if not(err_code == 0):
            return err_code
        else:
            try:
                # Creating a MongoClient Object
                # server.local_bind_port is assigned local port
                client = pymongo.MongoClient(
                    '127.0.0.1', server.local_bind_port)

                db = client[db_name]
                db.drop_collection(col_name)

                server.stop()
                return err_code
            except:
                err_code = 3
                server.stop()
                return err_code


def show_databases(pkey_path=cfg.PKEY_FILE_PATH):
    """
        Prints the structure of the MongoDB instance in text with the format:

        *database_name1 
            *collection_name1
            *collection_name2
        *database_name2
            *collection_name1
            *collection_name2
            *collection_name3
        *database_name3
            *collection_name1

        Parameters
        ----------
            pkey_path (optional): str
                full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
                on Linux, or wherever you saved it on Windows if saved via PuTTy 
        Returns
        -------
            err_code: int
                Code for any errors that might have occured, 0 signifies success.
    """
    # Creating SSH tunnel and opening SSH connection to database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        return err_code
    else:
        try:
            # Creating MongoClient object
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
            for db in client.list_database_names():
                print('*'+db)
                for col in client[db].list_collection_names():
                    doc_num = client[db][col].count()
                    print('\t*'+col+'--> # of docs: '+str(doc_num))
            server.stop()
            return err_code
        except:
            err_code = 3
            server.stop()
            return err_code


def doc_count(db_name, col_name, pkey_path=cfg.PKEY_FILE_PATH):
    """
        Returns the number of documents in collection 'col_name' which is located inside
        database 'db_name'

        Parameters
        ----------
        db_name: str
            name of the database that contains the collection to be counted
        col_name: str
            name of the collection to have the documents counted 
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy

        Returns
        -------
        docs: int
            Number of documents in the collection
        err_code: int
            Code for any errors that might have occured, 0 signifies success.

    """
    # Creating SSH tunnel and opening SSH connection to database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        return err_code
    else:
        try:
            # Creating a MongoClient Object
            # server.local_bind_port is assigned local port
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

            # Setting the target database and collection for stats
            db = client[db_name]
            cols_in_db = db.list_collection_names()
            if(col_name in cols_in_db):
                col = db[col_name]
                # get the document count (no filters) and print result
                docs = col.count()
                print('Number of documents in '+str(db_name) +
                      ' --> '+str(col_name)+': '+str(docs))

                # stop the server
                server.stop()

                return docs, err_code
            else:
                err_code = 12
                server.stop()
                return None, err_code
        except:
            err_code = 3
            server.stop()
            return None, err_code


def find_csv_headers(data_folder=cfg.DATA_FOLDER, csv_file_name=cfg.CSV_FILE_NAME):
    """
        All CSV headings are returned, so that they can be cross referenced with the existing
        column headers present in a collection within the database.

        Parameters
        ----------
        data_folder (optional): str
            Name of the folder where data is stored
        csv_file_name (optional): str
            Name of the CSV file in the project data/ folder where the data is stored
        Returns
        -------
        headers : list
            The list that has all the headings of the CSV file
        err_code: int
            Code for any errors that might have occured, 0 signifies success.
    """
    # getting the full path to the directory containing the csv data file
    dir_path = os.path.dirname(os.path.realpath(data_folder+csv_file_name))

    if os.name == 'nt':
        csv_file_path = dir_path + "\\" + csv_file_name  # If on Windows OS
    else:
        csv_file_path = dir_path + "/" + csv_file_name  # If on Linux OS

    # opening the csv file and reading the headers, if the file is not found, return 1
    if os.path.exists(csv_file_path):
        with open(csv_file_path, encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            csv_file.close()
        return headers, 0
    else:
        return None, 4


def return_existing_headers(db_name, col_name, pkey_path=cfg.PKEY_FILE_PATH):
    """
        Returns the JSON document keys (headers) present in collection 'col_name' of database 'db_name'. 
        This assumes that all documents within this collection have the same structure as the first document.

        Parameters
        ----------
        db_name : str
            Name of the database containing the collection whose headers are of interest
        col_name : str
            Name of the collection whose file headers are to be returned
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 

        Returns
        -------
        headers : list
            An array holding all the headers that are contained in documents
        err_code: int
            Code for any errors that might have occured, 0 signifies success.
    """
    # Establishing an SSH connection to the database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        return err_code
    else:
        try:
            # Creating a MongoClient Object
            # server.local_bind_port is assigned local port
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

            # Creating the necessary Database and Collection objects
            db = client[db_name]
            col = db[col_name]

            # Querying the collection for the first element and returning list of keys/headers

            result = list(col.find_one().keys())
            server.stop()
            return result, err_code
        # Exception handling if the SSH connection failed.
        except:
            err_code = 3
            server.stop()
            return None, err_code

    #from bson import Code
    #   def get_keys(db, collection):
    #        client = MongoClient()
    #        db = client[db]
    #       map = Code("function() { for (var key in this) { emit(key, null); } }")
    #        reduce = Code("function(key, stuff) { return null; }")
    #        result = db[collection].map_reduce(map, reduce, "myresults")
    #        return result.distinct('_id')


def compare_headers(csv_headers, col_headers):
    """
        Returns 1 if headers between the csv file and the ones in the documents stored in the collection are different. 
        This is mainly a function to print a warning to the user that headers in the csv might not be the same as those in the collection;
        in other words he would be adding a document with a different structure.

        Parameters
        ----------
        csv_headers: list
            All the headers in the csv data file to be uploaded
        col_headers: list
            All the views in the current database

        Returns
        ------- 
    """

    # Checking if the two lists of headers are the same
    if(csv_headers == col_headers):
        print('The headers are identical.')
    else:
        print('The headers have different entries')


def csv_to_json(data_folder=cfg.DATA_FOLDER, csv_file_name=cfg.CSV_FILE_NAME):
    """
        Converts the CSV of data to a JSON list with the intention of writing the JSON list directly to the
        desired database.

        First, os checks that `csv_file_path` exists. If not, an error code is returned. If it does, the
        csv file is opened and each row is turned into a python dictionary as an intermediary step. The rows, 
        which are then properly formatted JSON documents, are stored in a list and ready to be uploaded.

        Parameters
        ----------
        data_folder (optional): str
            Name of the folder where data is stored
        csv_file_name (optional): str
            The name of the csv file in the data/ folder.
        Returns
        -------
        data : list
            List of JSON documents ready to be uploaded to the database
        err_code: int
            Code for any errors that might have occured, 0 signifies success.


        Notes
        -----
        Keep in mind that 'csv_file_name' is the name of the target CSV file. 
        This is assuming the file is stored in the data folder. This data folder 
        should be inside the same directory as the DAQ script using the bvl_pymongodb module.
    """
    data = []
    # getting the full path to the directory containing the csv data file
    dir_path = os.path.dirname(os.path.realpath(data_folder+csv_file_name))
    os.chmod(dir_path, 0o777)
    if os.name == 'nt':
        csv_file_path = dir_path + "\\" + csv_file_name  # If on Windows OS
    else:
        csv_file_path = dir_path + "/" + csv_file_name  # If on Linux OS
    # If the file exists, we read each line as a dictionary and append it to a list
    if os.path.isfile(csv_file_path):
        with open(csv_file_path, encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for rows in csv_reader:
                data.append(rows)
            csv_file.close()
        return data, 0
    # If the file doesn't exist
    else:
        return None, 4


def get_latest_timestamp(db_name, col_name, pkey_path=cfg.PKEY_FILE_PATH):
    '''
        Function that returns the latest timestamp in a collection.

        Parameters
        ----------
        db_name: str
            The name of the database to which data is to be uploaded.
        col_name : str
            The name of the collection to which data is to be uploaded.
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 

    '''
    # Establishing an SSH connection to the database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        return err_code
    else:
        try:
            # Creating a MongoClient Object
            # server.local_bind_port is assigned local port
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

            # Creating the necessary Database and Collection objects
            db = client[db_name]
            col = db[col_name]

            # Querying the collection for the latest timestamp in the collection
            if(db_name == 'xgh-db'):
                result = list(col.find().sort(
                    'timestamp', -1).limit(1))[0]['timestamp'][:-4]
            else:
                result = list(col.find().sort(
                    'timestamp', -1).limit(1))[0]['timestamp']
            server.stop()
            return result, err_code
        # Exception handling if the SSH connection failed.
        except:
            err_code = 3
            server.stop()
            return None, err_code


def filter_std_condition(df, column, std_cut):
    '''
        Function that when givena dataframe, a target column, and a standard deviation cut
        for that column, calculates if the standard deviation of the data is greater than the cut

        Parameters
        ----------
        df : pandas.Dataframe
            dataframe with the data
        column : str
            The name of the column that is to be checked.
        Returns
        -------
        k : int
            0 if the std cut is larger than the data's, otherwise 1.        
    '''
    k = 0
    std = st.tstd(df[column])
    if std > std_cut:
        k += 1
    return k


def plot_stds(df, col_name, num_samples, plot_separators, return_arrays):
    '''
        Function that plots both the raw data in column 'col_name' of the Dataframe df and
        the standard deviation of intervals containing 'num_sample' datapoints. Separators can
        be plotted over the original data to delimit the intervals. Additionally, the arrays
        of standard deviations and separator locations can be returned.

        Parameters
        ----------
        df : pandas.Dataframe
            dataframe with the data
        col_name : str
            The name of the column that is to be checked.
        num_samples : int
            number of data points per interval
        plot_separators : Bool
            Whether to plot separators or not. 
        returns_arrays : Bool
            Whether to return the arrays of standard deviations and the array of indices
            for where the separators are located
        Returns
        -------
        std_arr, ind_arr : array tuple
            Tuple with array containing the standard deviations and an array with the indices
            that separate each interval
        i : int
            0 if the arrays are not to be returned.


    '''

    # Checking if the timestamp comes from the XGH DB or not
    # The XGH DB stores timestamps strings with an extra 3 decimal points of precision
    # So we have to specify that when converting back to a number of second since UNIX epoch
    if(len(df['timestamp'][0]) > 19):
        time_arr = [datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S.%f").timestamp()
                    for i in df['timestamp']]

    else:
        time_arr = [datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").timestamp()
                    for i in df['timestamp']]

    # We work with a copy of the original dataframe
    # we set the timestamp column to be the in seconds since the UNIX epoch
    # and we convert all data in the dataframe to floats (MongoDB often returns strings)
    temp_df = pandas.DataFrame.copy(df)
    time_arr = np.array(time_arr)
    temp_df['timestamp'] = time_arr
    temp_df = temp_df.astype(float)

    # We create a list of indices that delimit the intervals
    ind_arr = list(range(len(time_arr)))[::num_samples]
    ind_arr.append(len(time_arr)-1)
    std_arr = np.zeros(len(ind_arr)-1)

    # We calculate the std for each interval
    for i in range(1, len(ind_arr)):
        std_arr[i-1] = np.std(temp_df[col_name][ind_arr[i-1]:ind_arr[i]])

    # We start creating variables for our plots
    data_range = np.max(temp_df[col_name])-np.min(temp_df[col_name])
    fig, ax = plt.subplots(2)

    # Plotting the raw data
    ax[0].plot(time_arr-time_arr[0], temp_df[col_name])

    # Plotting the standard deviation points for each interval
    for i in range(1, len(ind_arr)):
        ax[1].plot(np.mean(time_arr[ind_arr[i-1]:ind_arr[i]] -
                   time_arr[0]), std_arr[i-1], marker='.', color='b')

    # plotting the separators if the user choses to show them
    if(plot_separators):
        for i in range(len(ind_arr)):
            ax[0].vlines(x=time_arr[ind_arr[i]]-time_arr[0], ymin=np.min(temp_df[col_name]) -
                         data_range*0.1, ymax=np.max(temp_df[col_name])+data_range*0.1, color='r')

    # Setting some axes titles
    fig.suptitle('Raw Data vs Standard Deviation for ' +
                 str(num_samples)+' Data Point Intervals')
    ax[0].set_ylabel('Original Data')
    ax[1].set_ylabel('Std')
    ax[1].set_xlabel('Time')

    # Returning the arrays of standard deviations and indices for the interval if the user choses to
    if(return_arrays == True):
        return std_arr, ind_arr
    else:
        return 0


def plot_sparsified(df, col_name, num_samples, stdev, plot_separators, return_arrays):
    '''
    Function that splits the raw data in column 'col_name' of the Dataframe df into intervals
    of 'num_samples' and plots the sparsified data if the std of the interval is above the 
    standard deviation cut. Additionally a plot that shows the original data is also featured; separators can
    be plotted over the original data to delimit the intervals. Furthermore, the regions where the
    standard deviation is above the cut have standard deviation points plotted in red. Finally, the arrays
    of standard deviations and separator locations can be returned.

    Parameters
    ----------
    df : pandas.Dataframe
        dataframe with the data
    col_name : str
        The name of the column that is to be checked.
    num_samples : int
        number of data points per interval
    stdev : float
        standard deviation cut
    plot_separators : Bool
        Whether to plot separators or not. 
    returns_arrays : Bool
        Whether to return the arrays of standard deviations and the array of indices
        for where the separators are located
    Returns
    -------
    std_arr, ind_arr : array tuple
        Tuple with array containing the standard deviations and an array with the indices
        that separate each interval
    i : int
        0 if the arrays are not to be returned.


    '''
    # Checking if the timestamp comes from the XGH DB or not
    # The XGH DB stores timestamps strings with an extra 3 decimal points of precision
    # So we have to specify that when converting back to a number of second since UNIX epoch
    if(len(df['timestamp'][0]) > 19):
        time_arr = [datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S.%f").timestamp()
                    for i in df['timestamp']]
    else:
        time_arr = [datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").timestamp()
                    for i in df['timestamp']]

    # We work with a copy of the original dataframe
    # we set the timestamp column to be the in seconds since the UNIX epoch
    # and we convert all data in the dataframe to floats (MongoDB often returns strings)
    temp_df = pandas.DataFrame.copy(df)
    time_arr = np.array(time_arr)
    temp_df['timestamp'] = time_arr
    temp_df = temp_df.astype(float)

    # We create a list of indices that delimit the intervals
    ind_arr = list(range(len(time_arr)))[::num_samples]
    ind_arr.append(len(time_arr)-1)
    std_arr = np.zeros(len(ind_arr)-1)

    # We calculate the std for each interval
    for i in range(1, len(ind_arr)):
        std_arr[i-1] = np.std(temp_df[col_name][ind_arr[i-1]:ind_arr[i]])

    # We start creating variables for our plots
    data_range = np.max(temp_df[col_name])-np.min(temp_df[col_name])
    fig, ax = plt.subplots(3)

    # Plotting the raw data
    ax[0].plot(time_arr-time_arr[0], temp_df[col_name])

    # Plotting the sparsified data for each interval
    for i in range(1, len(ind_arr)):
        if(std_arr[i-1] > stdev):
            ax[1].plot(time_arr[ind_arr[i-1]:ind_arr[i]]-time_arr[0], temp_df[col_name]
                       [ind_arr[i-1]:ind_arr[i]], '.', color='b', linestyle='')
        else:
            ax[1].plot(np.mean(time_arr[ind_arr[i-1]:ind_arr[i]]-time_arr[0]), np.mean(
                temp_df[col_name][ind_arr[i-1]:ind_arr[i]]), '.', color='b', linestyle='')

    # Plotting the standard deviation points for each interval
    for i in range(1, len(ind_arr)):
        if(std_arr[i-1] > stdev):
            ax[2].plot(np.mean(time_arr[ind_arr[i-1]:ind_arr[i]] -
                       time_arr[0]), std_arr[i-1], marker='.', color='r')
        else:
            ax[2].plot(np.mean(time_arr[ind_arr[i-1]:ind_arr[i]] -
                       time_arr[0]), std_arr[i-1], marker='.', color='b')

    # Plotting the separators if the user choses to show them
    if(plot_separators):
        for i in range(len(ind_arr)):
            ax[0].vlines(x=time_arr[ind_arr[i]]-time_arr[0], ymin=np.min(temp_df[col_name]) -
                         data_range*0.1, ymax=np.max(temp_df[col_name])+data_range*0.1, color='r')

    # Setting some axes titles
    fig.suptitle('Raw Data vs Standard Deviation for ' +
                 str(num_samples)+' Data Point Intervals')
    ax[0].set_ylabel('Original Data')
    ax[1].set_ylabel('Sparsified Data')
    ax[2].set_ylabel('Std')
    ax[2].set_xlabel('Time')

    # Returning the std array and the array of indices if the user wants
    if(return_arrays == True):
        return std_arr, ind_arr
    else:
        return 0


def plot_single_sparsified(x, y, num_samples, stdev):
    '''
    Function that plots raw data in y into intervals of 'num_samples' data points 
    and plots the sparsified data if the std of the interval is above the 
    standard deviation cut. 

    Parameters
    ----------
    x : pandas Series (dataframe column)
        the x data, normally df['timestamp']
    y : pandas Series (dataframe column)
        the y data, df['column_name']
    num_samples : int
        number of data points per interval
    stdev : float
        standard deviation cut

    Returns
    -------

    '''
    # Checking if the timestamp comes from the XGH DB or not
    # The XGH DB stores timestamps strings with an extra 3 decimal points of precision
    # So we have to specify that when converting back to a number of second since UNIX epoch
    if(len(df['timestamp'][0]) > 19):
        time_arr = [datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S.%f").timestamp()
                    for i in x]
    else:
        time_arr = [datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").timestamp()
                    for i in x]

    # Converting dataframe columns to np arrays
    time_arr = np.array(time_arr).astype(float)
    y = np.array(y).astype(float)

    # We create a list of indices that delimit the intervals
    ind_arr = list(range(len(time_arr)))[::num_samples]
    ind_arr.append(len(time_arr)-1)
    std_arr = np.zeros(len(ind_arr)-1)
    for i in range(1, len(ind_arr)):
        std_arr[i-1] = np.std(y[ind_arr[i-1]:ind_arr[i]])

    # plotting the sparsified data
    fig, ax = plt.subplots(1)
    for i in range(1, len(ind_arr)):
        if(std_arr[i-1] > stdev):
            ax.plot(x[ind_arr[i-1]:ind_arr[i]], y[ind_arr[i-1]:ind_arr[i]],
                    marker='.', color='b', linestyle='', markersize=1)
        else:
            ax.plot(np.mean(x[ind_arr[i-1]:ind_arr[i]]), np.mean(y[ind_arr[i-1]
                    :ind_arr[i]]), marker='.', color='b', linestyle='', markersize=1)


def sparsify_data(df, std_dict):
    # Checking if the data comes from the xgh db or not
    # if it does, it has millisecond precision so we need to convert the decimals in the timestamps too
    is_xgh = False
    if isinstance(df['timestamp'][0], pandas.Timestamp) and df['timestamp'][0].to_pydatetime().microsecond > 0:
        time_arr = [i.to_pydatetime().timestamp() for i in df['timestamp']]
        is_xgh = True
    else:
        time_arr = [i.timestamp() for i in df['timestamp']]

    # Creating a copy of the original dataframe, as the original dataframe is mutable
    # hence modifying its timestamp to be seconds since UNIX epoch in this function will modify it outside
    temp_df = pandas.DataFrame.copy(df)
    temp_df['timestamp'] = time_arr
    temp_df = temp_df.astype(float)

    # Verifying if the data in any of the columns changes enough
    k = 0
    for j in temp_df.columns[1:]:
        k += filter_std_condition(temp_df, j, std_dict[j])
        # if it does, we escape the loop and return the original dataframe
        if k > 0:
            break
    if k == 0:
        # If k==0 that means none of the columns have been determined to change enough
        # in that case we start computing the average of eacho column and make a new dataframe
        avg_list = []
        filtered_df = pandas.DataFrame(columns=temp_df.columns)
        for j in range(0, len(temp_df.columns)):
            avg_list.append(np.average(temp_df.iloc[:, j]))

            # Setting the first row of the dataframe to the averaged values of the columns
        filtered_df.loc[0] = avg_list

        # Fixing the timestamp to be in '%Y-%m-%d %H:%M:%S' format, making sure to match the proper precision
        # If we are working with a dataframe from the xgh-db
        if(is_xgh == 1):
            filtered_df['timestamp'] = datetime.datetime.fromtimestamp(
                filtered_df['timestamp'][0]).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        else:
            filtered_df['timestamp'] = datetime.datetime.fromtimestamp(
                filtered_df['timestamp'][0]).strftime('%Y-%m-%d %H:%M:%S').split('.')[0]

    else:
        # Returning the original dataframe if the data changed enough
        return df

    return filtered_df


def upload_from_csv(db_name, col_name, pkey_path=cfg.PKEY_FILE_PATH, data_folder=cfg.DATA_FOLDER,
                    csv_file_name=cfg.CSV_FILE_NAME, conflict_folder=cfg.CONFLICT_FOLDER, log_file=cfg.UPLOAD_LOG_FILE):
    """
        Uploads the contents of csv_file_name to the collection specified by col_name inside the database db_name.
        Performs an intermediate conversion of the the csv_file contents into a list, which contains the information
        to be uploaded in JSON document format. The contents of this list are then and uploaded to the database.


        Note: If the database db_name and the collection col_name do not already exist, calling this function will create them, 
        and upload the data as usual; therefore, it is important to correctly enter db_name and col_name.


        Parameters
        ----------
        db_name: str
            The name of the database to which data is to be uploaded.
        col_name : str
            The name of the collection to which data is to be uploaded.
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 
        data_folder (optional): str
            Name of the folder where data is stored.
        csv_file_name (optional): str
            The name of the csv file whose contents are to be uploaded to the database.
        conflict_folder (optional): str
            Name of the folder where conflict files get stored.
        log_file (optional): str
            Name of the file where last successful upload is tracked.
        Returns
        -------
        err_code: int
            Code for any errors that might have occured, 0 signifies success.

        Notes
        -----
        The file `csv_file_name` is expected to be in the data/ folder, which itself is in the same 
        directory as the script that performs the uploads to the database. If the csv is ever stored
        anywhere other than the project folder, the config file must be changed to redirect.

        In case of a write failure, the file will be stored in data/conflict/csv_file_name_conflict_YYYYMMDD_hhmmss.csv
        where _YYYYMMDD_hhmmss is the data in human readable format.
    """
    if (csv_file_name != cfg.CSV_FILE_NAME and col_name == "Slowcontrol"):
        response = input(
            'Are you sure you want to upload this data file to Slowcontrol? (y/n)')
        if(response != 'y'):
            return 11

    time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3]
    # Creating SSH tunnel and opening SSH connection to database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        # Creating conflict file
        conflict_file_name = csv_file_name.split(
            '.')[0]+'_conflict_'+time+'.'+csv_file_name.split('.')[1]
        # Checking if the conflict directory exists, if not, creating it and setting the failed upload as conflict
        if not (os.path.isdir(conflict_folder)):
            os.mkdir(conflict_folder)
        os.rename(data_folder+csv_file_name,
                  conflict_folder+conflict_file_name)
        open(data_folder+csv_file_name, 'x')
        return err_code
    else:
        try:
            # Creating a MongoClient Object
            # server.local_bind_port is assigned local port
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

            # Setting the target database and collection for the upload
            db = client[db_name]
            col = db[col_name]
        except:
            server.stop()
            return 3
        # logging initial collection document count and size
        init_col_count = db.command('collstats', col_name)['count']
        init_col_size = db.command('collstats', col_name)['size']

        # Creating the list of JSON documents to upload to the database
        data, err_code = csv_to_json(data_folder, csv_file_name)
        if not(err_code == 0):
            return err_code
        else:
            # Checking if the data from the csv was converted to a proper list of JSON objects
            # If the file contains no data
            if(type(data) == list and len(data) == 0):
                return 5
            # If the file exists and contains data
            else:
                col.insert_many(data)

                # Record the final number of documents and the size in bytes of the collection
                final_col_count = db.command('collstats', col_name)['count']
                final_col_size = db.command('collstats', col_name)['size']

                # Compare to see if number of documents increased by the expected amount
                # and if the size of the collection increased; this indicates a successful upload.
                if(final_col_size > init_col_size and final_col_count == init_col_count+len(data)):
                    # Writing to the log file that the upload was successful
                    log_time = datetime.datetime.now().strftime(
                        '%Y-%m-%d_%H:%M:%S.%f')[:-3]
                    with open(data_folder+log_file, 'w') as update_log:
                        update_log.write('Last successful upload: '+log_time)
                        update_log.close()
                    # Removing and remaking the upload file
                    os.remove(data_folder+csv_file_name)
                    open(data_folder+csv_file_name, 'x')
                    server.stop()
                    return err_code
                # In case of an unsuccessful upload or no change in document number
                else:
                    conflict_file_name = csv_file_name.split(
                        '.')[0]+'_conflict_'+time+'.'+csv_file_name.split('.')[1]
                    # Checking if the conflict directory exists, if not, creating it and setting the failed upload as conflict
                    if not (os.path.isdir(conflict_folder)):
                        os.mkdir(conflict_folder)
                    os.rename(data_folder+csv_file_name,
                              conflict_folder+conflict_file_name)
                    with open(data_folder+csv_file_name, 'x') as csv_create:
                        csv_create.close()
                    server.stop()
                    return 6


def get_data_as_csv(save_file_name, db_name, col_name, query_times, header_list=[],
                    pkey_path=cfg.PKEY_FILE_PATH, data_folder=cfg.DATA_FOLDER, download_folder=cfg.DOWNLOAD_FOLDER):
    """
        Queries the database 'db_name' and collection 'col_name' to return the data in columns whose names are 
        specified in 'header_list', for timestamp values between specified as ('initial_time','final_time') in a tuple.
        This corresponds to measurements that have occured between time 'initial_time' and 'final_time'. This function
        does not allow the user to overwrite the data file to prevent data loss.

        Parameters
        -----------
        save_file_name: str
            The name of the csv file to download the contents to.
        db_name: str
            The name of the database from which data is to be downloaded.
        col_name: str
            The name of the collection from which data is to be downloaded.
        query_time: tuple (start_time, stop_time)
            start_time (str) : the first timestamp for the query; the "start" time.
            stop_time (str) : the final timestamp for the query; the "stop" time.
            Format for the timestamps are: YYYY-MM-DD hh:mm:ss but YYYY-MM-DD also works if less precision is needed.
        header_list (optional): list 
            List with the column header names in str format, containing the data to be returned.
            Default : all columns are returned
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 
        data_folder (optional): str
            Name of the folder where the data is stored
            Default : DATA_FOLDER parameter in bvl_pymongodb_config
        Returns
        -------
        df : pandas Dataframe
            Dataframe containing the data returned by the query
        err_code: int
            Code for any errors that might have occured, 0 signifies success.

        Notes
    -----
    """

    log_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')[:-3]
    # Creating SSH tunnel and opening SSH connection to database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        return err_code
    else:
        # setting up the mongodb clinet and defining db and collection names
        try:

            # Creating a MongoClient Object
            # server.local_bind_port is assigned local port
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

            # Setting the target database and collection for the upload
            db = client[db_name]
            col = db[col_name]

        except:
            server.stop()
            return None, 3

        # set up and execute the mongo query
        try:
            # Creating the dictionary of headers to pass to the query function
            if(len(header_list) == 0):
                header_dict = {}  # Empty dictionary normally specifies to return ALL headers
                header_dict['_id'] = 0  # Add object's _id to be excluded
            else:
                # This line is for a custom header_list
                header_dict = {i: 1 for i in header_list}
                header_dict['_id'] = 0

            # Creating dictionary for the query time
            query_time_range = {'timestamp': {
                '$gt': query_times[0], '$lt': query_times[1]}}

            # Querying the database to obtain a Cursor item
            result = col.find(query_time_range, header_dict)

        except:
            server.stop()
            return None, 7

        # Convert from mongodb pointer to dataframe and write to csv
        try:
            # Transforming the iterator into a pandas Dataframe to write it to csv
            df = pandas.DataFrame(result)
            if(len(df) == 0):
                server.stop()
                return None, 13

            else:

                time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3]
                download_file_name = save_file_name.split(
                    '.')[0]+'_'+time+'.'+save_file_name.split('.')[1]
                # Writing the data to CSV
                if os.path.isfile(download_folder+download_file_name):
                    server.stop()
                    return df, 9
                else:
                    if not (os.path.isdir(data_folder)):
                        os.makedirs(download_folder)
                    elif (os.path.isdir(data_folder) and not os.path.isdir(download_folder)):
                        os.mkdir(download_folder)

                    with open(download_folder+download_file_name, 'w') as csv_writer:
                        csv_writer.write('Download date: '+log_time+'\n')
                        csv_writer.write('Database: '+db_name +
                                         ', Collection: '+col_name+'\n')
                        csv_writer.close()

                    df.to_csv(download_folder+download_file_name,
                              sep=',', mode='a', index=False)
                    server.stop()
                    return df, err_code

        except:
            server.stop()
            return None, 8


def get_data_as_df(db_name, col_name, query_times, header_list=[], pkey_path=cfg.PKEY_FILE_PATH):
    """
        Queries the database 'db_name' and collection 'col_name' to return the data in columns whose names are 
        specified in 'header_list', for timestamp values between specified as ('initial_time','final_time') in a tuple.
        This corresponds to measurements that have occured between time 'initial_time' and 'final_time'. 

        Parameters
        -----------
        db_name: str
            The name of the database from which data is to be downloaded.
        col_name: str
            The name of the collection from which data is to be downloaded.
        query_time: tuple (start_time, stop_time)
            start_time (str) : the first timestamp for the query; the "start" time.
            stop_time (str) : the final timestamp for the query; the "stop" time.
            Format for the timestamps are: YYYY-MM-DD hh:mm:ss but YYYY-MM-DD also works if less precision is needed.
        header_list (optional): list 
            List with the column header names in str format, containing the data to be returned.
            Default : all columns are returned
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 
        Returns
        -------
        df : pandas Dataframe
            Dataframe containing the data returned by the query
        err_code: int
            Code for any errors that might have occured, 0 signifies success.

        Notes
    -----
    """

    # Creating SSH tunnel and opening SSH connection to database PC
    server, err_code = ssh_connect(pkey_path)
    if not(err_code == 0):
        return None, err_code
    else:
        # setting up the mongodb clinet and defining db and collection names
        try:

            # Creating a MongoClient Object
            # server.local_bind_port is assigned local port
            client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

            # Setting the target database and collection for the upload
            db = client[db_name]
            col = db[col_name]

        except:
            server.stop()
            return None, 3

        # set up and execute the mongo query
        try:
            # Creating the dictionary of headers to pass to the query function
            if(len(header_list) == 0):
                header_dict = {}  # Empty dictionary normally specifies to return ALL headers
                header_dict['_id'] = 0  # Add object's _id to be excluded
            else:
                # This line is for a custom header_list
                header_dict = {i: 1 for i in header_list}
                header_dict['_id'] = 0

            # Creating dictionary for the query time
            query_time_range = {'timestamp': {
                '$gt': query_times[0], '$lt': query_times[1]}}

            # Querying the database to obtain a Cursor item
            result = col.find(query_time_range, header_dict)

        except:
            server.stop()
            return None, 7

        # Convert from mongodb pointer to dataframe and write to csv
        try:
            # Transforming the iterator into a pandas Dataframe
            df = pandas.DataFrame(result)

            server.stop()
            return df, err_code

        except:
            server.stop()
            return None, 8


def data_plotter(db_name, col_name, query_times, header_list=[], pkey_path=cfg.PKEY_FILE_PATH):
    '''
        Plots the desired columns contained in header_list from database db_name and collection col_name. Users must
        also pass the query times in a tuple to determine the time range of interest. IF the database query returns 
        and empty dataframe, the function will return an error code. If header_list is empty, all columns will be plotted.

        Parameters
        -----------
        db_name: str
            The name of the database from which data is to be downloaded.
        col_name: str
            The name of the collection from which data is to be downloaded.
        query_time: tuple (start_time, stop_time)
            start_time (str) : the first timestamp for the query; the "start" time.
            stop_time (str) : the final timestamp for the query; the "stop" time.
            Format for the timestamps are: YYYY-MM-DD hh:mm:ss but YYYY-MM-DD also works if less precision is needed.
        header_list (optional): list 
            List with the column header names in str format, containing the data to be returned.
            Default : all columns are returned
        pkey_path (optional): str
            full path to the file containing the DAQ computer's private key, typically found in /path/to/folder/.ssh/id_rsa
            on Linux, or wherever you saved it on Windows if saved via PuTTy 
        Returns
        -------
        err_code: int
            Code for any errors that might have occured, 0 signifies success.

    -----
    '''
    # Setting font for the plots
    font2 = {'family': 'serif', 'color': 'darkred', 'size': 15}

    # Querying the target dataframe
    df, error_code = get_data_as_df(
        db_name, col_name, query_times, header_list)
    if not (error_code == 0):
        return 13
    else:
        # Sparsifying the data in the df to increasing the plotting speed
        data_sparsification = np.ceil(len(df)/1250)
        ind_list = [i for i in range(len(df)) if i % data_sparsification == 0]
        df = df.loc[ind_list]

        # Sparsifying the tick marks (we want to fit max 10 tickmarks on the plot)
        ticks_sparsification = int(np.ceil(len(df)/10))

        # Looping through all non-timestamp columns
        for column in df.columns.values[1:]:
            # Making a list for our Y-data
            ypoints = df[column].astype(float).tolist()
            # Setting axis labels
            plt.xlabel("Time", fontdict=font2)
            plt.ylabel(column, fontdict=font2)
            ax = plt.gca()
            # plotting the data and the x-ticks
            plt.plot(range(len(df)), ypoints, linewidth=0.75)
            # makes the dates vertical to save space
            plt.xticks(range(len(df)),
                       df['timestamp'].tolist(), rotation='vertical')
            # will only print the date every nth entry
            ax.set_xticks(ax.get_xticks()[::ticks_sparsification])
            # plt.tight_layout()
            plt.show()

    '''#List of indices for every n-th data to create sparsification
    ind_list = [i for i in range(len(df)) if i%plotEveryNth==0]

    #Dataframe containing the sparsified data to plot
    df=df.iloc[ind_list]

    #Converting dataframe to JS readable list
    df = np.array(df.astype(float)).tolist()
    '''

    '''
    #Sparsifying the data in the df to increasing the plotting speed
    data_sparsification=int(np.ceil(len(df)/1250))
    df_data_avg=(df.iloc[:,1:].astype(float)).groupby(np.arange(len(df))//data_sparsification).mean()

    df_timestamps=df.iloc[:,0][::data_sparsification]
    df_timestamps=pd.DataFrame(data=df_timestamps.tolist(),columns=['timestamp'])
    df_final=df_timestamps.join(df_avgd)

    #Sparsifying the tick marks (we want to fit max 10 tickmarks on the plot)
    ticks_sparsification=int(np.ceil(len(df)/10))

    '''
