import sqlite3
import csv
from typing import Any

"""

CS205 Warmup Project
UVM Course Searching Tool
Authors: Nikhil, Eyu, Levi, Juli
Date: 9/25/2022

"""


def load_data(connection: sqlite3.Connection, cursor: sqlite3.Cursor):
    """ Loads the data from the CSV files into the database

    Args:
        connection (sqlite3.Connection): Connection to the Database
        cursor (sqlite3.Cursor): Cursor pointing to the Database
    """

    # Create Courses table
    cursor.execute("CREATE TABLE IF NOT EXISTS Courses ( \
                    id INTEGER PRIMARY KEY, \
                    number TEXT, \
                    teacher_id TEXT, \
                    title TEXT, \
                    credits INTEGER, \
                    level TEXT );")

    # Create Teachers table
    cursor.execute("CREATE TABLE IF NOT EXISTS Teachers ( \
                    id INTEGER PRIMARY KEY, \
                    name TEXT, \
                    email TEXT, \
                    title TEXT, \
                    expertise TEXT, \
                    office_location TEXT );")

    # If the Courses table is empty, read the csv file and add each course
    cursor.execute("SELECT * FROM Courses")
    if cursor.fetchall() == []:
        with open("courses.csv", 'r') as file1:
            reader1 = csv.reader(file1)
            header = next(reader1)
            if header != "":
                for row in reader1:
                    teacher_id = int(row[2])
                    credit = int(row[4])
                    cursor.execute(
                        f"INSERT INTO Courses (number, teacher_id, title, credits, level) VALUES ('{row[1]}', {teacher_id}, '{row[3]}', {credit}, '{row[5]}');")

    # If the Teachers table is empty, read the csv file and add each teacher
    cursor.execute("SELECT * FROM Teachers")
    if cursor.fetchall() == []:
        with open("teachers.csv", 'r') as file2:
            reader2 = csv.reader(file2)
            header = next(reader2)
            if header != "":
                for row in reader2:
                    cursor.execute(
                        f"INSERT INTO Teachers (name, email, title, expertise, office_location) VALUES ('{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}');")

    # Commit the SQL transaction to the database
    connection.commit()


def do_query(table_name: str, column_name: str, search_column: str, search_term: Any, cursor: sqlite3.Cursor):
    """ Queries the database using a SQL SELECT statement and returns the result

    Args:
        table_name (str): name of the table to query with the SELECT statement
        column_name (str): column to fetch from the table with the SELECT statement
        search_column (str): column for the WHERE clause
        search_term (Any): keyword to match on for the WHERE clause
        cursor (sqlite3.Cursor): Cursor object that points to the database

    Returns:
        str : String representation of the query result. Returns None if no records match the query
    """

    return_all = False
    return_part = False
    if search_column == 'size':
        # Meta data query for size of table
        cursor.execute(f'SELECT COUNT(id) FROM {table_name};')
        return_part = True
    elif search_column == '' and search_term == '':
        # Query for all Courses or all Teachers
        cursor.execute(f"SELECT {column_name} FROM {table_name};")
        return_all = True
    else:
        # Generic Query
        cursor.execute(f"SELECT {column_name} FROM {table_name} WHERE CAST({search_column} as TEXT) LIKE '{search_term}';")
        return_part = True
    result = cursor.fetchall()
    list_result = ""

    # Format the result(s) nicely as a string or return None if there are no results
    if return_part:
        if len(result) == 0:
            return None
        elif len(result) > 1:
            for x in result:
                list_result += str(x[0]) + ", "
            return list_result[:len(list_result) - 2]
        else:
            return result[0][0]
    elif (return_all):
        list_result += "+==============================================+\n"
        for i in result:
            if (table_name == "courses"):
                list_result += str(i[1]) + ": " + str(i[3]) + "\n"
            elif (table_name == "teachers"):
                list_result += f"{str(i[1]):<30} || {str(i[2]):<40} || {str(i[5]):<30}\n"
        list_result += "+==============================================+\n"
        return list_result[:len(list_result) - 2]

def check_empty_tables(cursor: sqlite3.Cursor):
    """Function to check if the SQL tables are empty

    Args:
        cursor (sqlite3.Cursor): Cursor object that points to the database

    Returns:
        bool: True if there are empty tables in the database
    """

    # Check if Courses table exists or if its empty
    cursor.execute("SELECT COUNT(*) FROM sqlite_Master WHERE type='table' AND name='Courses'")
    if cursor.fetchone()[0] == 0:
        return True
    else:
        cursor.execute("SELECT * FROM Courses")
        if cursor.fetchall() == []:
            return True

    # Check if Teachers table exists or if its empty
    cursor.execute("SELECT COUNT(*) FROM sqlite_Master WHERE type='table' AND name='Teachers'")
    if cursor.fetchone()[0] == 0:
        return True
    else:
        cursor.execute("SELECT * FROM Teachers")
        if cursor.fetchall() == []:
            return True
    
    return False


def print_instructions():
    # User Instructions for Query Format
    print("+==============================================+")
    print("|                 Query Format:                |")
    print("|                                              |")
    print("| Example Queries:                             |")
    print("|       - teacher of CS205                     |")
    print("|       - name of CS205                        |")
    print("|       - level of CS205                       |")
    print("|       - office of Jason Hibbeler             |")
    print("|       - expertise of Jason Hibbeler          |")
    print("|       - email of Jason Hibbeler              |")
    print("|       - title of Jason Hibbeler              |")
    print("|       - courses by Jason Hibbeler            |")
    print("|       - credits for CS205                    |")
    print("|       - title of Jason Hibbeler              |")
    print("|       - office teacher CS205                 |")
    print("|       - number of teachers/courses           |")
    print("|       - teachers/courses                     |")
    print("| Enter courses/teachers to get all the records|")
    print("|                                              |")
    print("| Rules:                                       |")
    print("|       - Queries must start with one of the   |")
    print("|         following: name, teacher, course.    |")
    print("|                                              |")
    print("|       - Queries can not be longer than 3     |")
    print("|         selections, an example of a query    |")
    print("|         that breaks this rule would be:      |")
    print("|         'teaching cs205 this semester'       |")
    print("|                                              |")
    print("|                                              |")
    print("+==============================================+")


def main():
    # connects to database and executes SQL statement
    connection = sqlite3.connect("info.db")
    cursor = connection.cursor()

    keep_going = "y"

    # Introduction to program
    print("+====================================================+")
    print("|         Welcome to Course Searcher                 |")
    print("|               Brought to you by                    |")
    print("|           Nikhil, Levi, Eyu, Juli                  |")
    print("| You can search all CS courses and teachers at UVM  |")
    print("|           Please Enter your Queries                |")
    print("|        If you need help use command 'help'         |")
    print("|      If you want to stop the software 'quit'       |")
    print("+====================================================+")

    # Loop to continue asking queries (Needs to be implemented)
    while keep_going == "y":

        empty_tables = check_empty_tables(cursor)

        # Query received and split up into an array
        query_full = input("Enter: ").replace('"', '')
        query_separated = query_full.split(' ', 2)

        # Fills in array if less than 3 long, needed to avoid errors in match statement
        if len(query_separated) < 3 and query_full != "help" and query_full != "load data" \
                and query_full != "quit":
            query_separated.append(" ")
            query_separated.append(" ")

        # Set parameters for do_query and verifies queries
        match query_separated[0]:
            case "load":
                # Loads data
                if query_full == "load data":
                    load_data(connection, cursor)
                    print("Data Loaded")
                    empty_tables = False
                    query_valid = True
            case "help":
                # prints instructions again
                if query_full == "help":
                    # Can make this a function to call that prints everything
                    # Would just help the match statement stay clean
                    print_instructions()
                    query_valid = True
            case "quit":
                if query_full == "quit":
                    keep_going = 'n'
                    query_valid = True
                else:
                    query_valid = False
            case "teacher":
                # teacher of {course number or course name}, result: teacher Name
                if query_separated[1] == "of":
                    table_name = "teachers"
                    column_name = "name"
                    search_column_name = "id"
                    if not empty_tables:
                        if len(query_separated[2]) == 5:
                            search_term = do_query("courses", "teacher_id", "number", query_separated[2], cursor)
                        else:
                            search_term = do_query("courses", "teacher_id", "title", query_separated[2], cursor)
                    query_valid = True
                else:
                    query_valid = False
            case "teachers":
                # return all the teachers
                if query_separated[1] == " ":
                    table_name = "teachers"
                    column_name = "*"
                    search_column_name = ""
                    search_term = ""
                    query_valid = True
                else:
                    query_valid = False
            case "office":
                # office of {teacher name}
                # office teacher {course number or course name}, result: office location
                table_name = "teachers"
                column_name = "office_location"
                if query_separated[1] == "of":
                    search_column_name = "name"
                    search_term = query_separated[2]
                    query_valid = True
                elif query_separated[1] == "teacher":
                    search_column_name = "id"
                    if not empty_tables:
                        if len(query_separated[2]) == 5:
                            search_term = do_query("courses", "teacher_id", "number", query_separated[2], cursor)
                        else:
                            search_term = do_query("courses", "teacher_id", "title", query_separated[2], cursor)
                    query_valid = True
                else:
                    query_valid = False
            case "email":
                # email of {teacher name}
                # email teacher {course number or course name}, result: email
                table_name = "teachers"
                column_name = "Email"
                if query_separated[1] == "of":
                    search_column_name = "name"
                    search_term = query_separated[2]
                    query_valid = True
                elif query_separated[1] == "teacher":
                    search_column_name = "id"
                    if not empty_tables:
                        if len(query_separated[2]) == 5:
                            search_term = do_query("courses", "teacher_id", "number", query_separated[2], cursor)
                        else:
                            search_term = do_query("courses", "teacher_id", "title", query_separated[2], cursor)
                    query_valid = True
                else:
                    query_valid = False
            case "expertise":
                # expertise of {teacher name}
                # expertise of {course number or course name}, result: expertise
                table_name = "teachers"
                column_name = "expertise"
                if query_separated[1] == "of":
                    search_column_name = "name"
                    search_term = query_separated[2]
                    query_valid = True
                elif query_separated[1] == "teacher":
                    search_column_name = "id"
                    if not empty_tables:
                        if len(query_separated[2]) == 5:
                            search_term = do_query("courses", "teacher_id", "number", query_separated[2], cursor)
                        else:
                            search_term = do_query("courses", "teacher_id", "title", query_separated[2], cursor)
                    query_valid = True
                else:
                    query_valid = False
            case "title":
                # title of {teacher name}
                # title teacher {course number or course name}, result: title
                table_name = "teachers"
                column_name = "title"
                if query_separated[1] == "of":
                    search_column_name = "name"
                    search_term = query_separated[2]
                    query_valid = True
                elif query_separated[1] == "teacher":
                    search_column_name = "id"
                    if not empty_tables:
                        if len(query_separated[2]) == 5:
                            search_term = do_query("courses", "teacher_id", "number", query_separated[2], cursor)
                        else:
                            search_term = do_query("courses", "teacher_id", "title", query_separated[2], cursor)
                    query_valid = True
                else:
                    query_valid = False
            case "courses":
                if len(query_separated) == 3:
                    # courses by {teacher name}, result: course names with that teacher_id
                    if query_separated[1] == "by":
                        table_name = "courses"
                        column_name = "title"
                        search_column_name = "teacher_id"
                        if not empty_tables:
                            search_term = do_query("teachers", "ID", "name", query_separated[2], cursor)
                        query_valid = True
                    # return all the courses
                    elif query_separated[1] == " ":
                        table_name = "courses"
                        column_name = "*"
                        search_column_name = ""
                        search_term = ""
                        query_valid = True
                    else:
                        query_valid = False
            case "course":
                # course suggestion
                if query_separated[1] == "suggestions":
                    table_name = "courses"
                    column_name = "title"
                    search_column_name = "teacher_id"
                    if not empty_tables:
                        search_term = do_query("teachers", "ID", "name", "Jason Hibbeler", cursor)
                    query_valid = True
                else:
                    query_valid = False
            case "credits":
                # credits for {course number or course name}, result: credits
                if query_separated[1] == "for":
                    table_name = "courses"
                    column_name = "credits"
                    search_term = query_separated[2]
                    if len(search_term) > 5:
                        search_column_name = "title"
                    else:
                        search_column_name = "number"
                    query_valid = True
                else:
                    query_valid = False
            case "level":
                # level of {course number or course name}, results: level
                if query_separated[1] == "of":
                    table_name = "courses"
                    column_name = "level"
                    search_term = query_separated[2]
                    if len(search_term) > 5:
                        search_column_name = "title"
                    else:
                        search_column_name = "number"
                    query_valid = True
                else:
                    query_valid = False
            case "name":
                # name of {course number}, results: course title
                if query_separated[1] == "of":
                    table_name = "courses"
                    column_name = "title"
                    search_column_name = "number"
                    search_term = query_separated[2]
                    query_valid = True
                else:
                    query_valid = False
            case "number":
                if query_separated[1] == "of" and query_separated[2] == "teachers":
                    # number of teachers
                    table_name = "teachers"
                    column_name = ""
                    search_column_name = "size"
                    search_term = ""
                    query_valid = True
                elif query_separated[1] == "of" and query_separated[2] == "courses":
                    # number of courses
                    table_name = "courses"
                    column_name = ""
                    search_column_name = "size"
                    search_term = ""
                    query_valid = True
                else:
                    query_valid = False
            case _:
                query_valid = False

        # do_query called if query valid, if not error message is printed
        if query_valid:
            if query_full != "load data" and query_full != "help" and query_full != "quit":
                if(empty_tables):
                    print("Your database is empty. Please run the command 'load data' before querying the database.")
                else:
                    result = do_query(table_name, column_name, search_column_name, search_term, cursor)
                    if result == None:
                        print("Uh oh! Your query returned no results. Perhaps the teacher/course you are searching for has a typo?")
                    else:
                        print(result)
        else:
            # User didn't enter a valid query - which is needed
            print("ERROR: You entered a non-valid query statement")

    print("Thanks for using the Course Searcher. Goodbye.")

    connection.close()

    return 0


main()
