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
    df_exams.sort_values(by='Year', inplace = True)
    # END SOLUTION
    return df_exams


def ex2(df_students):
    """
    return a df frame with the degree count
    # NOTE -- rename name the degree column to Count!!!
    """
    # BEGIN SOLUTION
    df = df_students['Degree'].value_counts().to_frame()
    df.columns = [["Count"]]
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
    # df3 = pd.merge(df_studentexamscores, df_exams, on = 'Exam').groupby(['Exam','Year']).mean().round(2).reset_index()
    # df3 = df3.sort_values(by = 'Score', ascending = False).drop('StudentID', axis = 1)
    # df3 = df3.rename(columns = {'Score': 'average'})
    # df3.reset_index(drop=True, inplace = True)
    # END SOLUTION
    df = pd.merge(df_studentexamscores, df_exams, on = 'Exam')
    df_average = df.groupby(["Exam", "Year"])["Score"].mean().round(2).reset_index()
    df_average.sort_values(by = "Score", ascending = False, inplace = True)
    #df_average['Score'] = df_average['Score'].astype('int32')
    df_average["Score"].astype("int32")
    df_average["Year"] = df_average["Year"].astype("int32")


    df_average = df_average.rename(columns = {"Score": "average"})
    df_average.set_index("Exam", inplace = True)
    return df_average


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
    df = pd.merge(df_studentexamscores, df_students, on = 'StudentID').groupby(['Degree']).mean().round(2)
    df = df.rename(columns = {'Score': 'Average'})
    df = df['Average'].to_frame()
    return df
    # END SOLUTION
    

def ex5(df_studentexamscores, df_students):
    """
    merge df_studentexamscores and df_students to produce the output below. The output shows the average of the top 
    10 students in descending order. 
    Hint: https://stackoverflow.com/a/20491748
    round to two decimal places

    """

    # BEGIN SOLUTION
    df5 = pd.merge(df_studentexamscores, df_students, on = 'StudentID').groupby(['StudentID']).mean().round(2).reset_index()
    df_sort = df5.sort_values(by = 'Score', ascending = False)
    df_sort = df_sort.rename(columns = {'Score': 'average'})
    df6 = pd.merge(df_sort, df_students, on = 'StudentID')
    df = df6.loc[0:9,['First_Name', 'Last_Name', 'Degree','average']]
    return df
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
    first_name = []
    last_name = []
    user_name = []
    for _ in range(100):
        name = fake.name().split(" ")
        num = str(np.random.randint(1000,9999))
        first_name.append(name[0])
        last_name.append(' '.join(name[1:]))
        u_name = name[0].lower()[0:2]+num
        #print(u_name)
        user_name.append(u_name)
        #print([np.random.randint(1000,9999) for i in range(4)])

    dic = {'username' : user_name, 'first_name': first_name , 'last_name': last_name}
    df = pd.DataFrame.from_dict(dic)
    return df
    # END SOLUTION
    


def part2_step2():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    mu = [35, 75, 25, 45, 45, 75, 25, 45, 35]
    sigma = [9, 15, 7, 10, 5, 20, 8, 9, 10]
    max_score = [50, 100, 40, 60, 50, 100, 50, 60, 50]

    # Generate the scores using a normal distribution
    scores = np.random.normal(mu, sigma, size = (100,9))
    scores = np.round(scores)
    scores_clip = np.clip(scores,0,max_score)
    lst = scores_clip.T
    dic = {'Hw1' : lst[0] ,'Hw2': lst[1],'Hw3': lst[2], 'Hw4': lst[3], 'Hw5': lst[4], 'Exam1': lst[5], 'Exam2': lst[6], 'Exam3':lst[7] , 'Exam4': lst[8]}
    df2 = pd.DataFrame.from_dict(dic)
    return df2
    # END SOLUTION


def part2_step3(df2_scores):
    # BEGIN SOLUTION
    mu = [35, 75, 25, 45, 45, 75, 25, 45, 35]
    sigma = [9, 15, 7, 10, 5, 20, 8, 9, 10]
    df_des = df2_scores.describe().T.drop(['count','min','25%','50%','75%','max'], axis = 1).round(2)
    df_des['mean_theoretical'] = mu
    df_des['std_theoretical'] = sigma
    df_des['abs_mean_diff'] = abs(df_des['mean'] - df_des['mean_theoretical']).round(2)
    df_des['abs_std_diff'] = abs(df_des['std'] - df_des['std_theoretical']).round(2)
    return df_des
    # END SOLUTION


def part2_step4(df2_students, df2_scores, ):
    # BEGIN SOLUTION
    max_score = [50, 100, 40, 60, 50, 100, 50, 60, 50]
    df2_students['Hw1'] = ((df2_scores['Hw1']/max_score[0])*100).round()
    df2_students['Hw2'] = ((df2_scores['Hw2']/max_score[1])*100).round()
    df2_students['Hw3'] = ((df2_scores['Hw3']/max_score[2])*100).round()
    df2_students['Hw4'] = ((df2_scores['Hw4']/max_score[3])*100).round()
    df2_students['Hw5'] = ((df2_scores['Hw5']/max_score[4])*100).round()
    df2_students['Exam1'] = ((df2_scores['Exam1']/max_score[5])*100).round()
    df2_students['Exam2'] = ((df2_scores['Exam2']/max_score[6])*100).round()
    df2_students['Exam3'] = ((df2_scores['Exam3']/max_score[7])*100).round()
    df2_students['Exam4'] = ((df2_scores['Exam4']/max_score[8])*100).round()
    return df2_students.round()
    # END SOLUTION


def part2_step5():
    # BEGIN SOLUTION
    df = pd.read_csv('part2_step5-input.csv')
    dfhw2 = df[df.iloc[:,4] == 'AI_ISSUE']
    dfhw3 = df[df.iloc[:,5] == 'AI_ISSUE']
    dfhw4 = df[df.iloc[:,6] == 'AI_ISSUE']
    dfhw5 = df[df.iloc[:,7] == 'AI_ISSUE']
    dfex1 = df[df.iloc[:,8] == 'AI_ISSUE']
    dfex2 = df[df.iloc[:,9] == 'AI_ISSUE']
    dfex3 = df[df.iloc[:,10] == 'AI_ISSUE']
    dfex4 = df[df.iloc[:,11] == 'AI_ISSUE']
    pd5 = pd.concat([dfhw2,dfhw3,dfhw4,dfhw5,dfex1,dfex2,dfex3,dfex4], axis=0)
    pd5['index1'] = pd5.index
    pd6 = pd5.groupby(pd5.columns.tolist(),as_index=False).size()
    pd6.sort_values(by = 'index1',inplace = True)
    #print(pd6)
    pd6 = pd6.rename(columns ={'size':'AI_Count'})
    return pd6[['username','first_name','last_name','AI_Count']].reset_index(drop = True)

    # END SOLUTION


def part2_step6():
    # BEGIN SOLUTION
    df = pd.read_csv('part2_step5-input.csv')
    pd.replace()    
    # END SOLUTION
