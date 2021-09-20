import asyncio
import aiohttp
import re
from urllib import request
from datetime import date

counter = 0

def URLtoString(URL):
    a=request.urlopen(URL)
    b=a.read()
    c=b.decode("UTF-8")
    return c

def SMString_To_Exact_URLs(SubMainString, SubMainURL):
    StringList=[]
    x=0
    counter=0
    while True:
        if re.search('............csv', SubMainString[x:]):
            SmallString=re.search('............csv', SubMainString[x:]).group()
            if counter>=1 and SubMainURL+'/'+SmallString==StringList[counter-1]:
                x=x+re.search('............csv', SubMainString[x:]).end()
                continue
            else:
                StringList.append(SubMainURL+'/'+SmallString)
                counter+=1
                x=x+re.search('............csv', SubMainString[x:]).end()
        else:
            break
    return StringList

def do_something(results, year):
    with open('Data/' + str(year) + '.txt', 'w') as file:
        for r in results:
            file.write(r[309:])

async def read(url, year, URLs):
    global counter
    session = aiohttp.ClientSession()
    async with session.get(url) as response:
        text = await response.text("UTF-8")
        await session.close()
        counter += 1
        if int(counter/len(URLs) * 100) != int((counter - 1)/len(URLs) * 100):
            print(str(int(counter/len(URLs) * 100)) + ' %  ' + 'from ' + str(year))
        if int((counter/len(URLs)) * 100) >= 99:
                print(counter)
        return text

async def Insert_Data_To_File(URLs, year):
    jobs = [asyncio.create_task(read(url, year, URLs)) for url in URLs]
    do_something(await asyncio.gather(*jobs), year)

def main():
    MainURL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access"
    year = int(input('Start Year: '))
    end_year = int(input('End Year: '))
    while year <= end_year:
        SubMainURL = MainURL+'/'+str(year)
        SubMainString = URLtoString(SubMainURL)
        URLs=SMString_To_Exact_URLs(SubMainString, SubMainURL)
        global counter
        counter = 0
        #asyncio.get_event_loop().run_until_complete(Insert_Data_To_File(URLs, file, year))
        asyncio.run(Insert_Data_To_File(URLs, year))
        year += 1

if __name__ == '__main__':
  main()
