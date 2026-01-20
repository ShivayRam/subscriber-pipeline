"""
pipeline.py

This script:
- Reads raw subscriber data from the dev folder
- Cleans and standardizes student/career/job data
- Writes cleaned outputs to the prod folder
- Appends to a changelog file
- Logs details to a logfile

Usage:
    python src/pipeline.py
"""

import sqlite3
import pandas as pd
import ast
import numpy as np
import os
import logging


#PATHS - use project-relative directories

# Determine script directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Project root: one directory up
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, os.pardir))

# Data directories
DEV_DIR = os.path.join(PROJECT_ROOT, "dev")
PROD_DIR = os.path.join(PROJECT_ROOT, "prod")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Ensure directories exist
os.makedirs(DEV_DIR, exist_ok=True)
os.makedirs(PROD_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# File locations
RAW_DB_PATH = os.path.join(DEV_DIR, "cademycode.db")
CLEANSED_DB_PATH = os.path.join(PROD_DIR, "cademycode_cleansed.db")
CLEANSED_CSV_PATH = os.path.join(PROD_DIR, "cademycode_cleansed.csv")
CHANGELOG_PATH = os.path.join(PROJECT_ROOT, "changelog.md")


#LOGGING

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "cleanse_db.log"),
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
    force=True
)

logger = logging.getLogger(__name__)


#DATA CLEANSING FUNCTIONS

def cleanse_student_table(df):

    """
    Clean and prepare the student table.
    Converts columns, handles nulls, expands JSON–like fields.

    Returns:
        tuple: (cleaned_df, nulls_df)
    """

    today = pd.to_datetime('today')

    df['dob'] = pd.to_datetime(df['dob'], errors='coerce') #convert to datetime

    df['age'] = (
        today.year 
        - df['dob'].dt.year 
        - (
            (today.month < df['dob'].dt.month) | 
            ((today.month == df['dob'].dt.month) & 
             (today.day < df['dob'].dt.day))
        )
    )

    #bucket into age groups of 10 years
    df['age_group'] = np.int64((df['age']/10))*10

    #json normalize to explode into dictionary
    df['contact_info'] = df['contact_info'].apply(lambda x: ast.literal_eval(x))
    explode_contact = pd.json_normalize(df['contact_info'])
    df = pd.concat([df.drop('contact_info', axis=1).reset_index(drop=True), explode_contact], axis=1)

    #split mailing address
    split_add = df.mailing_address.str.split(',', expand=True)
    split_add.columns = ['street', 'city', 'state', 'zip_code']
    df = pd.concat([df.drop('mailing_address', axis=1), split_add], axis=1)

    #convert object datatypes to float where needed
    df['job_id'] = df['job_id'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float)
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)

    #collect null rows for reporting
    null_data = pd.DataFrame()

    #drop rows with null num_course_taken
    null_course_taken = df[df[['num_course_taken']].isnull().any(axis=1)]
    null_data = pd.concat([null_data, null_course_taken])
    df = df.dropna(subset=['num_course_taken'])

    #drop rows with null job_id
    null_job_id = df[df[['job_id']].isnull().any(axis=1)]
    null_data = pd.concat([null_data, null_job_id])
    df = df.dropna(subset=['job_id'])

    #replace remaining nulls in career_path_id / time_spent
    df['current_career_path_id'] = np.where(df['current_career_path_id'].isnull(), 0, df['current_career_path_id'])
    df['time_spent_hrs'] = np.where(df['time_spent_hrs'].isnull(), 0, df['time_spent_hrs'])

    return (df, null_data)


def cleanse_career_path(df):

    """
    Add an 'not_applicable' career path to handle missing student values.
    """

    #add new career path id to account for students not taking a career path
    not_applicable = {'career_path_id': 0, 'career_path_name': 'not_applicable', 'hours_to_complete': 0}

    df.loc[len(df)] = not_applicable

    return(df)

def cleanse_student_jobs(df):

    """
    Remove duplicates from the student jobs table.
    """

    return(df.drop_duplicates())



#UNIT TESTS

def test_nulls(df: pd.DataFrame):

    """
    Ensure no null rows remain in the cleaned dataset.
    """

    df_null = df[df.isnull().any(axis=1)]
    cnt_null = len(df_null)

    try:
        assert cnt_null == 0, 'There are ' + str(cnt_null) + " nulls in the table"

    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    
    else:
        print('No null rows found')


def test_schema(local_df: pd.DataFrame, db_df: pd.DataFrame):

    """
    Check that schemas match between local and DB versions.
    """

    errors = 0
    for col in db_df:
        try:
            if local_df[col].dtypes != db_df[col].dtypes:
                errors += 1
        except NameError as ne:
            logger.exception(ne)
            raise ne
    
    if errors > 0:
        assert_error_msg = str(errors) + "Column(s) dtypes aren't the same"
        logger.exception(assert_error_msg)

    assert errors == 0, assert_error_msg


def test_num_cols(local_df: pd.DataFrame, db_df: pd.DataFrame):

    """
    Ensure number of columns matches between local and DB tables.
    """

    try:
        assert len(local_df.columns) == len(db_df.columns)

    except  AssertionError as ae:
        logger.exception(ae)
        raise ae
    
    else:
        print("Number of columns are the same")

def test_for_path_id(students: pd.DataFrame, career_paths: pd.DataFrame):

    """
    Check career_path_id referential integrity.
    """

    student_table = students.current_career_path_id.unique()
    is_subset = np.isin(student_table, career_paths.career_path_id.unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing career_path_id(s): " + str(list(missing_id)) + " in `career_paths` table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("All career_path_ids are present")

def test_for_job_id(students: pd.DataFrame, student_jobs: pd.DataFrame):

    """
    Check job_id referential integrity.
    """

    student_table = students.job_id.unique()
    is_subset = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing job_id(s): " + str(list(missing_id)) + " in `student_jobs` table"

    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    
    else:
        print("All job_ids are present")


#MAIN PIPELINE LOGIC

def main():
    # Read existing changelog lines if file exists
    if os.path.exists(CHANGELOG_PATH):
        with open(CHANGELOG_PATH, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    # Determine next version
    if len(lines) == 0:
        next_ver = 0
    else:
        try:
            # Example first line: "## 0.0.3"
            first_line = lines[0].strip()
            version_str = first_line.split()[-1]        # e.g., "0.0.3"
            # Extract last integer part after the final dot
            version_segments = version_str.split(".")
            next_ver = int(version_segments[-1]) + 1
        except Exception:
            next_ver = 0


    con = sqlite3.connect(RAW_DB_PATH)
    students = pd.read_sql_query("SELECT * FROM cademycode_students", con)
    career_paths = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
    student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
    con.close()

    try:
        con = sqlite3.connect(CLEANSED_DB_PATH)
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", con)
        missing_db = pd.read_sql_query("SELECT * FROM incomplete_data", con)
        con.close()
        
        new_students = students[~np.isin(students.uuid.unique(), clean_db.uuid.unique())]

    except:
        new_students = students
        clean_db = []

    clean_new_students, missing_data = cleanse_student_table(new_students)

    try:
        new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(), missing_db.uuid.unique())]
    except:
        new_missing_data = missing_data

    if len(new_missing_data) > 0:
        sqlite_connection = sqlite3.connect(CLEANSED_DB_PATH)
        new_missing_data.to_sql('incomplete_data', sqlite_connection, if_exists='append', index=False)
        sqlite_connection.close()

    if len(clean_new_students) > 0:
        clean_career_paths = cleanse_career_path(career_paths)
        clean_student_jobs = cleanse_student_jobs(student_jobs)

        test_for_job_id(clean_new_students, clean_student_jobs)
        test_for_path_id(clean_new_students, clean_career_paths)

        df_clean = clean_new_students.merge(
            clean_career_paths,
            left_on='current_career_path_id',
            right_on='career_path_id',
            how='left'
        )

        df_clean = df_clean.merge(
            clean_student_jobs,
            on='job_id',
            how='left'
        )

        if len(clean_db) > 0:
            test_num_cols(df_clean, clean_db)
            test_schema(df_clean, clean_db)
        
        test_nulls(df_clean)

        sqlite_connection = sqlite3.connect(CLEANSED_DB_PATH)
        df_clean.to_sql('cademycode_aggregated', sqlite_connection, if_exists='append', index=False)
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", sqlite_connection)
        sqlite_connection.close()

        clean_db.to_csv(CLEANSED_CSV_PATH)

        new_lines = [
            f"## 0.0.{next_ver}\n",
            "### Added\n",
            f"- {len(df_clean)} cleansed rows added\n",
            f"- {len(new_missing_data)} missing rows recorded\n",
            "\n"
        ]

        with open(CHANGELOG_PATH, 'w') as f:
            f.writelines(new_lines + lines)


    else:
        print("no new data")
        logger.info("no new data")
    logger.info("End Log")
    

if __name__ == "__main__":
    main()