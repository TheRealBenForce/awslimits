import urllib2
from bs4 import BeautifulSoup
import boto3
import datetime


def make_limits_list() :
    # make soup
    time = datetime.datetime.now().isoformat()
    limitslink = 'https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html'
    page = urllib2.urlopen(limitslink)
    soup = BeautifulSoup(page, 'html.parser')

    
    # all and only limit sections begin with an h2 tag on the site
    # parse h2 tags for topicsname
    # create list of questions until finding next h2 tag
    allh2 = soup.find_all('h2')
    limitslist = []
    i = 0
    for h2 in allh2 :
        dtopic = {}
        dtopic["Topic"] = h2.getText(" ", strip=True).replace('\\n','')
        dtopic["Topic"] = ' '.join(dtopic["Topic"].split())
        dtopic["Topic"] = dtopic["Topic"]
        dtopic["Questions"] = []
        dquest = {}
        t = h2.find_next("table")
        question_num = 1
        for row in t.findAll("tr"):
            cells = row.findAll("td")
            if len(cells) > 0:
                dquest["Number"] = question_num
                dquest["Question"] = repr(cells[0].getText(" ", strip=True)).replace('\\n','')
                dquest["Question"] = ' '.join(dquest["Question"].split())
                dquest["Question"] = dquest["Question"][2:-1]
                dquest["Answer"] = repr(cells[1].getText(" ", strip=True)).replace('\\n','')
                dquest["Answer"] = ' '.join(dquest["Answer"].split())
                dquest["Answer"] = dquest["Answer"][2:-1]
                dquest["Time"] = time
                if len(dquest["Answer"]) > 0 and  len(dquest["Question"]) > 0 :
                    dtopic["Questions"].append(dict(dquest))
                    question_num += 1
        limitslist.append(dict(dtopic))
    return limitslist 

l = make_limits_list()    
print(l[0])

def make_table(mylist) : 
    client = boto3.client('dynamodb')
    for d in mylist:
        #d = mylist[0]
        topic = d['Topic']
        questions = d['Questions']
        for q in questions :
            client.put_item(
                TableName='AwsLimits',
                Item={
                    'Topic':{'S': topic },
                    'Question':{'S': (q['Question'])},
                    'QuestionNumber':{'N': str(q['Number'])},
                    'Answer':{'S': q['Answer']},
                    'Time':{'S':q['Time']}
                }
            )
    return

make_table(l)
