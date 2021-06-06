from time import sleep, time
import re
import csv

import mysql.connector
from mysql.connector import errorcode, DataError, IntegrityError, DatabaseError

a = time()


def make_one_to_many(TableName, Element, localElementId, argsNames_for_sql_insert):
    sql = "Select * from " + TableName
    mycursor.execute(sql)
    myresult = mycursor.fetchall()

    currentElementID = None

    if Element != "":
        if Element in [cell[1] for cell in myresult]:
            pass
            currentElementID = [cell[0] for cell in myresult if cell[1] == Element][0]
        else:
            localElementId += 1
            currentElementID = localElementId
            sql = "INSERT INTO " + TableName + " (" + argsNames_for_sql_insert + ") VALUES (%s,%s)"
            val = (localElementId, Element)
            mycursor.execute(sql, val)
            mydb.commit()

    return currentElementID, localElementId




try:
    mydb = mysql.connector.connect(
        user='kacza',
        password='Pomidorowa4',
        host='127.0.0.1',
        database="seattlelibrary"
    )
    mycursor = mydb.cursor()

    sql = "DELETE FROM seattlelibrary.isbn;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.items_subjects;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.subjects;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.items;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.itemstypes;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.publishers;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.authors;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.itemscollections;"
    mycursor.execute(sql)
    sql = "DELETE FROM seattlelibrary.locations;"
    mycursor.execute(sql)

    mydb.commit()


    # mydb.close()
    # exit(0)
#   +       +      +      +         +               +          +         +            +             +               +
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
        fileIsEnded = False
        count = 1
        print(file.readline())

        subject_primary_key = 0
        book_primary_key = 0
        currentPublisherID = 0
        localAuthorID = 0
        localPublisherID = 0
        localItemTypesID = 0
        localItemLocationID = 0
        localItemCollectionID = 0
        localSubjectId = 0

        while not fileIsEnded:
            try:
                l = file.readline()

                # wujek bob bylby zly
                if not l:
                    for i in range(10):
                        l = file.readline()
                        if l:
                            break
                    else:
                        print("good job")
                        break

                try:
                    print(count)
                    line = ['"{}"'.format(x) for x in
                            list(csv.reader([l], delimiter=',', quotechar='"'))[0]]


                    try:
                        BibNum = int(line[0][1:-1])

                    except ValueError:
                        continue

                    Title = str(line[1][1:-1])

                    AuthorNames = line[2][1:-1]
                    currentAuthorID, localAuthorID = make_one_to_many("Authors", AuthorNames, localAuthorID,
                                                                            "AuthorID,names")


                    PublicationYear = line[4][1:-1]

                    try:
                        PublicationYear = int(re.search("[0-9]{4}", PublicationYear.split(",")[0]).group(0))
                    except AttributeError:
                        count += 1
                        continue

                    Publisher = line[5][1:-1]
                    currentPublisherID, localPublisherID = make_one_to_many("Publishers", Publisher, localPublisherID,
                                                                            "PublisherID,name")
                    ItemType = line[7][1:-1]
                    currentItemTypeID, localItemTypesID = make_one_to_many("ItemsTypes", ItemType, localItemTypesID,
                                                                           "ItemTypeId,name")
                    ItemCollection = line[8][1:-1]
                    currentItemCollectionID, localItemCollectionID = make_one_to_many("ItemsCollections", ItemCollection, localItemCollectionID,
                                                                           "ItemCollectionID,name")

                    FloatingItem = line[9][1:-1]
                    if FloatingItem == "NA":
                        FloatingItem = 0
                    else:
                        FloatingItem = 1

                    ItemLocation = line[10][1:-1]

                    currentItemLocationID, localItemLocationID = make_one_to_many("locations",
                                                                                      ItemLocation,
                                                                                      localItemLocationID,
                                                                                      "LocationID,name")
                    #

                    ReportDate = line[11][1:-1]
                    ItemCount = line[12][1:-1]

                    sql = "INSERT INTO items (itemID,BibNum, Title, Author,PublicationYear,publisher,itemtype,itemcollection,FloatingItem,Location,ItemCount,ReportDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val = (count, str(BibNum), Title, currentAuthorID, int(PublicationYear), currentPublisherID,
                           currentItemTypeID,currentItemCollectionID,FloatingItem,currentItemLocationID,ItemCount,ReportDate)
                    mycursor.execute(sql, val)
                    mydb.commit()

                    ISBNs = line[3][1:-1]

                    if ISBNs == "":
                        isbn = None
                    else:
                        for isbn in ISBNs.split(","):
                            isbn.replace(" ", "")
                            sql = "INSERT INTO isbn (item,isbn) VALUES (%s,%s)"
                            val = (count, isbn)
                            mycursor.execute(sql, val)
                            mydb.commit()
                    #


                    Subjects = line[6][1:-1]
                    Subjects = Subjects.split(",")
                    for subject in Subjects:
                        try:
                            localSubjectId += 1
                            sql = "INSERT INTO subjects (subjectID,name) VALUES (%s,%s)"
                            val = (localSubjectId, subject)
                            mycursor.execute(sql, val)
                            mydb.commit()
                        except IntegrityError:
                            sql = "Select subjectID from Subjects where Subjects.name = '" + subject + "'"
                            mycursor.execute(sql)
                            myresult = mycursor.fetchone()
                            currentSubjectID = myresult[0]
                        else:
                            currentSubjectID = localSubjectId

                        sql = "INSERT INTO items_subjects (ItemID,SubjectID) VALUES (%s,%s)"
                        val = (count, currentSubjectID)
                        mycursor.execute(sql, val)
                        mydb.commit()


                    # print(count)
                    count += 1

                except IndexError:
                    pass

            except UnicodeDecodeError:
                pass
finally:
    mydb.close()
b = time()

print(b - a)
