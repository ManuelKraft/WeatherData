from datetime import date
import Download
import Edit_Data
import asyncio


def Get_Days_From_Edited_String(Row):
    x = 0
    counter  = 0
    while Row[x + counter:].find(',') != -1:
        x += Row[x + counter:].find(',') 
        counter += 1
        if counter == 1:
            z = x + counter - 1
        elif counter == 2:
            a = Row[z + 1:x + counter - 1]
            return 31 * int(a[5:7]) + int(a[8:10])

def Get_ID_From_Edited_String(Row):
    return Row[:Row.find(',')]

def Get_Days_From_Raw_String(Row):
    x = 0
    counter  = 0
    while Row[x + counter:].find(',') != -1:
        x += Row[x + counter:].find(',') 
        counter += 1
        if counter == 1:
            z = x + counter - 1
        elif counter == 2:
            a = Row[z + 2:x + counter - 2]
            return 31 * int(a[5:7]) + int(a[8:10])

def Get_ID_From_Raw_String(Row):
    return Row[1:Row.find(',')]

actual_date = str(date.today())
input = input('Enter Name of most current edited data textfile:  ')

StationID = []; Date = []
List1 = [StationID, Date]
DoubleList = []; DoubleList2 = []; DoubleList3 = []
counter = 0
f = open('Data_Edited/' + str(input) + '.txt', 'r')
x = f.tell()
file_length = len(f.readlines())
f.seek(x)
while True:
    row = f.readline()
    if not row:
        break
    if len(List1[0]) == 0:
        List1[0].append(Get_ID_From_Edited_String(row))
        List1[1].append(Get_Days_From_Edited_String(row))
    elif List1[0][-1] != Get_ID_From_Edited_String(row):
        List1[0].append(Get_ID_From_Edited_String(row))
        List1[1].append(Get_Days_From_Edited_String(row))
    elif List1[0][-1] == Get_ID_From_Edited_String(row) and List1[1][-1] < Get_Days_From_Edited_String(row):
        List1[1][-1] = Get_Days_From_Edited_String(row)
    counter += 1
    if round(((counter - 1)/file_length) * 100, 0) != round((counter/file_length) * 100, 0):
        print('Preparing...  ' + str(int(round((counter/file_length) * 100, 0))) + ' %')
f.close()
'''
print('Start Downloading...')
MainURL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access"
SubMainURL = MainURL+'/'+str(date.today().year)
SubMainString = Download.URLtoString(SubMainURL)
URLs=Download.SMString_To_Exact_URLs(SubMainString, SubMainURL)
counter = 0
asyncio.run(Download.Insert_Data_To_File(URLs, date.today().year))
print('Download Finished!')'''

print('Start Updating...')
f = open('Data/' + str(date.today().year) + '.txt', 'r')
x = f.tell()
file_length = len(f.readlines())
f.seek(x)
filename = 'from_' + input[-10:] + '_until_' + str(date.today()) + '.txt'
g = open('Data_Edited/' + filename, 'w')
g2 = open('Data_Edited/Table1/' + filename, 'w')
g3 = open('Data_Edited/Table2/' + filename, 'w')
DoubleList = []
AttributeList = Edit_Data.Create_Attribute_Lists_From_Twin_Stations()
CountryString = Edit_Data.Create_Country_String()
KeyList = Edit_Data.Create_Key_List()
counter = 0; counter2 = 0
while True:
    Row = f.readline()
    if not Row:
        break
    j = 0
    exist_id = False
    while j < len(List1[0]):
        if Row.find(List1[0][j]):
            exist_id = True
            if Get_Days_From_Raw_String(Row) > List1[1][j]:
                Row2 = Edit_Data.Insert_Country_Column(Row)
                Row3 = Edit_Data.Delete_Columns(Row2)
                Row4 = Edit_Data.Convert_Units(Row3)
                Row5 = Edit_Data.Convert_Code_To_Country(Row4, CountryString)
                Row6 = Edit_Data.Edit_Chars(Row5)
                Row7 = Edit_Data.Edit_Wrong_Data(Row6)
                Row8 = Edit_Data.Edit_Double_IDs(Row7, AttributeList)
                if Edit_Data.Delete_Useless_Data(Row8) == False:
                    g.write(Row8)
                    Edit_Data.Adjust_For_Table1(Row8, g2)
                    if not Edit_Data.Key_Exists(Row8, KeyList):
                        Edit_Data.Adjust_For_Table2(Row8, g3)
                counter2 += 1
                #print(counter2)
                break
            break
        j += 1
    if exist_id == False:
        Row2 = Edit_Data.Insert_Country_Column(Row)
        Row3 = Edit_Data.Delete_Columns(Row2)
        Row4 = Edit_Data.Convert_Units(Row3)
        Row5 = Edit_Data.Convert_Code_To_Country(Row4, CountryString)
        Row6 = Edit_Data.Edit_Chars(Row5)
        Row7 = Edit_Data.Edit_Wrong_Data(Row6)
        Row8 = Edit_Data.Edit_Double_IDs(Row7, AttributeList)
        if Edit_Data.Delete_Useless_Data(Row8) == False:
            g.write(Row8)
            Edit_Data.Adjust_For_Table1(g2, Row8)
            Edit_Data.Adjust_For_Table2(g3, Row)
        print("New_ID: ", counter2)
        counter2 += 1
    counter += 1
    if round(((counter - 1)/file_length) * 100, 0) != round((counter/file_length) * 100, 0):
        print('Updating...  ' + str(int(round((counter/file_length) * 100, 0))) + ' %')
print('Updates finished!\nAdded Rows: ' + str(counter2))
g.close()
f.close()
print('Insert Added Rows into Database: ')
Edit_Data.Insert_Into_Database(filename)



    
    
    
