import pandas as pd
import os

def read_file(filename, path=None):
    """
    Reads a file into memory and returns a dataframe.

    Args:
        filename (str): name of file to read.
        path (str, optional): path to the file to read. Defaults to None.

    Raises:
        ValueError: File type is not supported.
        ValueError: Invalid file name
        Exception: Any other error message
    """
    df = pd.DataFrame()
    if len(filename) > 1:
        try:
            if not path:
                path = 'Input'

            file_type = filename.split('.')[1]
            long_file_name = os.path.join(path, filename) 
            if file_type == 'csv':
                df = pd.read_csv(long_file_name, low_memory=False)
            elif file_type in ['xls', 'xlsx']:
                df = pd.read_excel(long_file_name)
            else:
                raise ValueError(f"{file_type} File type is not supported")
        except Exception as e:
            print('read_file: ' + repr(e))

    else:
        raise ValueError("Invalid File Name")

    return df

def ensure_directory_exist(directory):
    """ Checks if the given directory exists. Creates the directory if it does not exist.

    Args:
        directory (str): The name of the directory to check.
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except Exception as e:
        print('ensure_directory_exist: ' + repr(e))

def save_as_csv(df, filename, path=None, allow_index=False):
    """ Saves a given dataframe to a CSV file.

    Args:
        df (DataFrame): The dataframe to be saved
        filename (str): The name given to the CSV file
        path (str, optional): Path to where to save the file. Defaults to None.
        allow_index (bool, optional): If True, Save existing dataframe index as a column in the saved CSV file. Defaults to False.
    """
    try:
        if path:
            ensure_directory_exist(path)
        else:
            path = ''
            
        if 'csv' not in filename:
            filename = filename.split('\\')[-1].split('.')[0] + '.csv'
        
        long_file_name = os.path.join(path, filename.replace(' ', '_'))
        
        df.to_csv(path_or_buf=long_file_name, index=allow_index)
        # return long_file_name
        print(f"{filename} saved successfully!")
    except Exception as e:
        print('save_as_csv: ' + repr(e))

def remove_special_characters(text):
    """ Remove special characters from a string.

    Args:
        text (str): Source text to remove special characters.

    Returns:
        [str]: Text with special characters removed.
    """
    txt = ""
    try:
        txt = text.translate({ord(c): "_" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+"})
        txt = txt.replace(' ', '_')
    except Exception as e:
        print('remove_special_characters: ' + repr(e))
    return txt

def get_df_subset_from_col_range(df, start_col, end_col):
    """ Select a subset from a dataframe given a range of columns.

    Args:
        df (DataFrame): The DataFrame to select from.
        start_col (str): The start column range to select from.
        end_col (str): The end column range to select from.

    Returns:
        DataFrame: The subset of the DataFrame selected
    """
    dd = pd.DataFrame()
    try:
        dd = df.loc[:, start_col:end_col]
    except Exception as e:
        print('get_df_subset_from_col_range: ' + repr(e))
    
    return dd

def create_year_month_day_column(dataframe, date_column):
    df = dataframe
    try:

        dt = pd.to_datetime(df[date_column])
        df[date_column]= dt 

        df['year'] = pd.DatetimeIndex(df[date_column]).year
        df['month'] = df[date_column].dt.month_name(locale='English')
        df['day_name'] = df[date_column].dt.day_name()
    except Exception as e:
        print('create_year_month_day_column: ' + repr(e))

    return df

def merge_dataframe(df1, df2, left_on, right_on, sort=False, right_suffix=None, drop_rcolumns=[]):
    df = pd.DataFrame()
    try:
        if right_suffix:
            right_suffix = '_' + right_suffix
        else:
            right_suffix = '_y'

        cols_to_use = df2.columns.difference(df1.columns).tolist()
        if len(cols_to_use) == 0:
            cols_to_use = df2.columns.tolist()
        elif right_on not in cols_to_use:
            cols_to_use.append(right_on)

        cols_to_use = [col for col in cols_to_use if col not in drop_rcolumns]    
        df = df1.merge(df2[cols_to_use],left_on=left_on, 
                        right_on=right_on, sort=sort,
                        suffixes=('', right_suffix)).drop(
                        columns=[right_on + right_suffix])
    except Exception as e:
        print('merge_dataframe: ' + repr(e))
    return df

def append_dataframe(df1, df2, sort=False):
    pass