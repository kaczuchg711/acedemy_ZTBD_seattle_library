from time import sleep, time
import re
import csv

import mysql.connector
from mysql.connector import errorcode

try:
    mydb = mysql.connector.connect(
        user='kacza',
        password='Pomidorowa4',
        host='127.0.0.1',
        database="seattlelibrary"
    )
    mycursor = mydb.cursor()
    sql = "DELETE FROM seattlelibrary.items;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.authors;"
    mycursor.execute(sql)
    mydb.commit()

# BibNum, Title, Author, ISBN, PublicationYear, Publisher, Subjects, ItemType, ItemCollection, FloatingItem, ItemLocation, ReportDate, ItemCount
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    with open("library-collection-inventory.csv", "r") as file:
        with open("file.txt", "w") as out:
            # todo BibNum is not unique add primary key to item table
            count = 0
            fileIsEnded = False
            print(file.readline())
            subjects_local = []
            subject_primary_key = 0
            book_primary_key = 0

            subject_primary_key = 0

            localauthorId = 0


            while not fileIsEnded:
                # print("count: ", count)
                try:
                    try:
                        line = ['"{}"'.format(x) for x in list(csv.reader([file.readline()], delimiter=',', quotechar='"'))[0]]

                        try:
                            BibNum = int(line[0][1:-1])

                        except ValueError:
                            continue

                        Title = str(line[1][1:-1])
                        AuthorNames = line[2][1:-1]
                        sql = "Select * from Authors"
                        mycursor.execute(sql)
                        myresult = mycursor.fetchall()
                        currentAuthorID = None
                        if AuthorNames != "":
                            if AuthorNames in [cell[1] for cell in myresult]:
                                pass
                                currentAuthorID = [cell[0] for cell in myresult if cell[1] == AuthorNames][0]
                            else:
                                localauthorId += 1
                                currentAuthorID = localauthorId
                                sql = "INSERT INTO Authors (authorid,names) VALUES (%s,%s)"
                                val = (localauthorId, AuthorNames)
                                mycursor.execute(sql, val)
                                mydb.commit()

                        # Items
                        sql = "INSERT INTO items (BibNum, Title, Author) VALUES (%s, %s, %s)"
                        val = (str(BibNum), Title, currentAuthorID)
                        mycursor.execute(sql, val)
                        mydb.commit()

                        ISBN = line[3][1:-1]
                        PublicationYear = line[4][1:-1]

                        try:
                            PublicationYear = int(re.search("[0-9]{4}", PublicationYear.split(",")[0]).group(0))
                        except AttributeError:
                            continue

                        Publisher = line[5][1:-1]
                        Subjects = line[6][1:-1]
                        Subjects = Subjects.split(",")
                        # Books-Subjects -> BibNum, subject_number
                        for subject in Subjects:
                            subject = subject.lstrip()
                            if subject not in subjects_local:
                                subjects_local.append(subject)

                        ItemType = line[7][1:-1]
                        ItemCollection = line[8][1:-1]
                        FloatingItem = line[9]

                        if FloatingItem == "NA":
                            FloatingItem = False
                        else:
                            FloatingItem = True
                        ItemLocation = line[10][1:-1]

                        ReportDate = line[11][1:-1]

                        ItemCount = line[12][1:-1]

                        if count == 1000:
                            break

                        count += 1

                    except IndexError:
                        pass

                except UnicodeDecodeError:
                    pass
finally:
    mydb.close()
