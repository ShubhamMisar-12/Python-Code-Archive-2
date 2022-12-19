import sqlite3
import numpy as np
import pandas as pd
from faker import Faker


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


conn = create_connection('non_normalized.db')
sql_statement = "select * from Students;"
df = pd.read_sql_query(sql_statement, conn)
print(df)


def create_df_degrees(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_degrees' dataframe that contains only
    the degrees. See screenshot below. 
    """

    # BEGIN SOLUTION
    conn = create_connection(non_normalized_db_filename)
    sql_statement = "SELECT DISTINCT Degree FROM Students"
    df = pd.read_sql_query(sql_statement, conn)
    conn.close()
    return df
    # END SOLUTION


def create_df_exams(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_exams' dataframe that contains only
    the exams. See screenshot below. Sort by exam!
    hints:
    # https://stackoverflow.com/a/16476974
    # https://stackoverflow.com/a/36108422
    """

    # BEGIN SOLUTION
    exam_year = []
    for index, data in df.iterrows():
        lst = data.values
        my_lst = list(map(lambda x: (x.split(" ")[0], int(x.split(" ")[1][1:-1])), lst[3].split(', ')))
        #print(my_lst)
        for i in my_lst:
            if i not in exam_year:
                exam_year.append(i)
    dic = {'Exam' : [ey[0] for ey in exam_year], 'Year': [ey[1] for ey in exam_year]}
    df2 = pd.DataFrame.from_dict(dic)
    return df2.sort_values(by=['Exam']).reset_index(drop = True)

    

    # END SOLUTION


def create_df_students(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_students' dataframe that contains the student
    first name, last name, and degree. You will need to add another StudentID column to do pandas merge.
    See screenshot below. 
    You can use the original StudentID from the table. 
    hint: use .split on the column name!
    """

    # BEGIN SOLUTION
    df[['Last_Name','First_Name']] = df['Name'].str.split(', ', expand=True)
    df['StudentID'] = df['StudentID'].apply(lambda x: int(x))
    return df[['StudentID','First_Name','Last_Name','Degree'] ]
    
    # END SOLUTION


def create_df_studentexamscores(non_normalized_db_filename, df_students):
    """
    Open connection to the non-normalized database and generate a 'df_studentexamscores' dataframe that 
    contains StudentID, exam and score
    See screenshot below. 
    """

    # BEGIN SOLUTION
    student_exam_scores = []
    for index, data in df.iterrows():
        lst = data.values    
        my_lst = list(map(lambda x: (x.split(" ")[0]), lst[3].split(', ')))
        my_lst2 = list(map(lambda x: int(x.split(" ")[0]), lst[4].split(', ')))
        for i in range(0,len(my_lst)):
            student_exam_scores.append([int(lst[0]),my_lst[i], my_lst2[i]])
    dic = {'StudentID' : [ey[0] for ey in student_exam_scores], 'Exam': [ey[1] for ey in student_exam_scores], 'Score': [ey[2] for ey in student_exam_scores]}
    return pd.DataFrame.from_dict(dic)
    # END SOLUTION


def ex1(df_exams):
    """
    return df_exams sorted by year
    """
    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df_exams


def ex2(df_students):
    """
    return a df frame with the degree count
    # NOTE -- rename name the degree column to Count!!!
    """
    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df


def ex3(df_studentexamscores, df_exams):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the exams. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df


def ex4(df_studentexamscores, df_students):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the degrees. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    # BEGIN SOLUTION
    pass
    # END SOLUTION
    return df


def ex5(df_studentexamscores, df_students):
    """
    merge df_studentexamscores and df_students to produce the output below. The output shows the average of the top 
    10 students in descending order. 
    Hint: https://stackoverflow.com/a/20491748
    round to two decimal places

    """

    # BEGIN SOLUTION
    pass
    # END SOLUTION


# DO NOT MODIFY THIS CELL OR THE SEED

# THIS CELL IMPORTS ALL THE LIBRARIES YOU NEED!!!


np.random.seed(0)
fake = Faker()
Faker.seed(0)


def part2_step1():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    fake = Faker()
    Faker.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    pass
    # END SOLUTION
    


def part2_step2():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step3(df2_scores):
    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step4(df2_students, df2_scores, ):
    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step5():
    # BEGIN SOLUTION
    pass
    # END SOLUTION


def part2_step6():
    # BEGIN SOLUTION
    pass
    # END SOLUTION
