from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib
import json
import sqlalchemy as sa
import pandas as pd
import datetime
import os
import numpy as np

# Get the database URL from the environment (as required by DigitalOcean App Platform)
DATABASE_URL = os.getenv('DATABASE_URL')
myerror = {"myerror": "Δεν έχετε δικαίωμα πρόσβασης."}

class MyRequestHandler(BaseHTTPRequestHandler):

    def set_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')  # You can replace '*' with a specific domain if needed
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
    
    def do_OPTIONS(self):
        # Handle preflight requests (OPTIONS method)
        self.send_response(200)
        self.set_cors_headers()
        self.end_headers()

    def do_GET(self):
        # Parse URL and parameters
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)

        # Set CORS headers for all responses
        self.set_cors_headers()

        # Determine the endpoint
        if parsed_path.path == '/getReports':
            self.getReports(query_params)
        elif parsed_path.path == '/getReport':
            self.getReport(query_params)
        elif parsed_path.path == '/getSampleM16':
            self.getSampleM16(query_params)
        elif parsed_path.path == '/getHistorySampleM16':
            self.getHistorySampleM16(query_params)
        elif parsed_path.path == '/getAvailablePaymentsM16':
            self.getAvailablePaymentsM16(query_params)
        elif parsed_path.path == '/createSampleM16':
            self.createSampleM16(query_params)
        elif parsed_path.path == '/getSampleM193':
            self.getSampleM193(query_params)
        elif parsed_path.path == '/createSampleM193':
            self.createSampleM193(query_params)
        elif parsed_path.path == '/searchDiavgeia':
            self.searchDiavgeia(query_params)
        elif parsed_path.path == '/createFundsXML':
            self.createFundsXML(query_params)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Endpoint not found.")

    def createFundsXML(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # SQL query to get user info with correct parameterization using %s for psycopg2
            get_user_query = "SELECT * FROM public.tools_rights WHERE funds=1 AND username=(%(username)s)"
            user_info = pd.read_sql_query(get_user_query, con=engine, params={"username": username})
    
            # If user exists, return their information, else return error
            if len(user_info) > 0:
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                eif1 = json.loads(request.args.get('eif1'))
                eif1 = pd.DataFrame.from_records(eif1, columns=eif1[0])
                eif1 = eif1.iloc[1:]
                eif2 = json.loads(request.args.get('eif2'))
                eif2 = pd.DataFrame(eif2, columns=eif2[0])
                eif2 = eif2.iloc[1:]
                esif = pd.merge(left=eif1, right=eif2, on="Transaction PSKE code")
                funds = request.args.get('funds')
                funds = json.loads(funds.encode('utf8'))
                funds = pd.DataFrame.from_records(funds, columns=funds[0])
                funds = funds.iloc[1:]
                data = pd.merge(left=funds, right=esif, left_on=u'Κωδικός Πρότασης', right_on="Transaction PSKE code")
                data.rename(columns={u'Περιφέρεια': 'CD_REGION', u'ΚΑΔ Κωδικός': 'CD_KAD_EPENDISIS',
                                     'Transaction PSKE code': 'REQ_ID', 'LOAN_TYPE': 'DRASI',
                                     'Total Eligible Project Cost 1': 'INVEST_AMNT', 'Transaction principal amount': 'REQ_AMNT',
                                     u'Επωνυμία': 'NAME', u'ΑΦΜ': 'AFM', u'MUNICIPAL_DISTRICT': 'LAU2', u'ΤΚ': 'ADDR_ZIP',
                                     'Total Turnover': 'COMPANY_SIZE', 'Number of employees': 'EMPLOYEE_AMNT',
                                     'Transaction reference': 'AR_CONTRACT', 'Transaction signature date': 'DT_SIGN_CONTRACT',
                                     'Transaction maturity': 'REQ_MONTH', 'Gross Grant Equivalent': 'AIE',
                                     'Cumulated disbursed principal amount': 'AMNT_TAKE',
                                     'End of disbursement period (Y/N)': 'INVEST_COMPLETE'}, inplace=True)
                data["STATUS_EVAL"] = '1'
                data["BANK_STATUS_REQ"] = '1'
                data["ETEAN_STATUS_REQ"] = '1'
                data["BUSINESS_PROGRAM"] = '1'
                data.loc[data['INVEST_COMPLETE'] == 'Yes', 'INVEST_COMPLETE'] = '1'
                data.loc[data['INVEST_COMPLETE'] == 'No', 'INVEST_COMPLETE'] = '0'
                evaluated = data
                evaluated = evaluated[["REQ_ID", "CD_BANKHD", "CD_REGION", "DRASI", "CD_KAD_EPENDISIS",
                                       "INVEST_AMNT", "REQ_AMNT", "NAME", "AFM", "COMPANY_SIZE", "EMPLOYEE_AMNT",
                                       "LAU2", "ADDR_ZIP"]]
                signed = data
                signed = signed[
                    ["REQ_ID", "CD_BANKHD", "REQ_AMNT", "NAME", "AFM", "COMPANY_SIZE", "AR_CONTRACT", "DT_SIGN_CONTRACT",
                     "REQ_MONTH", "AIE"]]
                disbursed = data
                disbursed = disbursed[
                    ["REQ_ID", "CD_BANKHD", "REQ_AMNT", "NAME", "AFM", "COMPANY_SIZE", "AR_CONTRACT", "DT_SIGN_CONTRACT",
                     "REQ_MONTH", "AIE", "INVEST_COMPLETE"]]
                evaluated = evaluated.to_json(orient="records")
                signed = signed.to_json(orient="records")
                disbursed = disbursed.to_json(orient="records")
                # Create the JSON structure
                response_data = {
                    "evaluated": evaluated,
                    "signed": signed,
                    "disbursed": disbursed
                }
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def searchDiavgeia(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # SQL query to get user info with correct parameterization using %s for psycopg2
            get_user_query = "SELECT * FROM public.tools_rights WHERE m16sampling=1 AND username=(%(username)s)"
            user_info = pd.read_sql_query(get_user_query, con=engine, params={"username": username})
    
            # If user exists, return their information, else return error
            if len(user_info) > 0:
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                terms = json.loads(request.args.get('applicationsData'))
                to_date = today
                from_date = today - datetime.timedelta(days=5000)
                to_date = str(to_date)
                from_date = str(from_date)
                decisions = pd.DataFrame([])
                for term in terms:
                    if ((str(term[0]).isdigit()) and (len(str(term[0]))>5)):
                        url = "https://diavgeia.gov.gr/luminapi/opendata/search.json?term=" + str(term[0]) + "&from_date="+from_date+"&to_date="+to_date
                        response = urllib.urlopen(url)
                        html = response.read()
                        html = json.loads(html)
                        if len(html['decisions']) > 0 :
                            for i in range(len(html['decisions'])):
                                decisions = pd.concat([decisions, pd.DataFrame.from_records([
                                    {'term': str(term[0]), 'ada': html['decisions'][i]['ada'],
                                     'date': html['decisions'][i]['issueDate'], 'subject': html['decisions'][i]['subject'],
                                     'url': html['decisions'][i]['documentUrl']}])])
                        else:
                            decisions = pd.concat([decisions, pd.DataFrame.from_records([
                                {'term': str(term[0]), 'ada': "Δε βρέθηκε αναρτημένη απόφαση για αυτόν τον Κωδικό ΟΠΣΑΑ",
                                 'date': "", 'subject': "", 'url': ""}])])
                    else:
                        decisions = pd.concat([decisions, pd.DataFrame.from_records([
                            {'term': str(term[0]), 'ada': "Μη έγκυρη τιμή Κωδικού ΟΠΣΑΑ",
                             'date': "", 'subject': "", 'url': ""}])])
                decisions = decisions.to_dict(orient='records')
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(decisions).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def createSampleM193(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()
            now = datetime.datetime.now()
            now = now.strftime("%d/%m/%Y %H:%M:%S")

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            get_user = """SELECT * FROM public.tools_rights WHERE m193sampling=1 AND username=(%(username)s)"""
            users = pd.read_sql_query(get_user, con=engine, params={"username": username})

            # If user exists, return their information, else return error
            if (len(users) > 0):
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                data = request.args.get('data')
                data = json.loads(data.encode('utf8'))
                data = pd.DataFrame.from_records(data, columns=data[0])
                data = data.iloc[1:]
                data = data.reset_index()
                print(data)
                if (set(['opsaa','name','amount','final']).issubset(data.columns)):
                    if any(data['opsaa'].apply(lambda x: len(str(x)) != 10)):
                        print(data['opsaa'])
                        engine.dispose()
                        temperror = {"myerror": "Οι κωδικοί ΟΠΣΑΑ δεν είναι στη σωστή μορφή."}
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(json.dumps(temperror).encode())
                    elif data.isnull().values.any():
                        temperror = {"myerror": "Το αρχείο περιέχει κενές τιμές."}
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(json.dumps(temperror).encode())
                    elif is_numeric_dtype(data['amount']):
                        temperror = {"myerror": "Η στήλη amount πρέπει να περιέχει μόνο αριθμητικές τιμές."}
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(json.dumps(temperror).encode())
                    elif not data['final'].isin([0,1]).all():
                        temperror = {"myerror": "Οι τιμές της στήλης final πρέπει να είναι μόνο 0 ή 1."}
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(json.dumps(temperror).encode())
                    else:
                        get_previous = """SELECT DISTINCT opsaa FROM public.m193sampling WHERE selectedsample='Ναι'"""
                        previous = pd.read_sql_query(get_previous, con=engine, params={"value": "Ναι"})
                        previous['selectedsample'] = "Επιλέχθηκε σε προηγούμενο δείγμα"
                        data['opsaa'] = data['opsaa'].astype(str)
                        previous['opsaa'] = previous['opsaa'].astype(str)
                        data = pd.merge(data, previous, on="opsaa", how="left")
                        dataToSample = data.loc[data['selectedsample']!='Επιλέχθηκε σε προηγούμενο δείγμα']
                        dataRest = data.loc[data['selectedsample']=='Επιλέχθηκε σε προηγούμενο δείγμα']
                        dataFinalPayment = data.loc[data['final'] == 1]
                        dataToSample = dataToSample.loc[data['final'] != 1]
                        if len(dataToSample) == 0:
                            temperror = {"myerror": "Δεν προέκυψαν έργα στο Δείγμα. Ελέγξτε ξανά το αρχείο έργων που επισυνάψατε."}
                            self.send_response(404)
                            self.end_headers()
                            self.wfile.write(json.dumps(temperror).encode())
                        else:
                            if set(['opsaa','name']).issubset(dataToSample.columns):
                                sample = dataToSample.sample(frac=0.1, random_state=1)
                                if (len(sample) == 0):
                                    sample = dataToSample.sample(n=1, replace=True, random_state=1)
                                sample = pd.concat([sample, dataFinalPayment])
                                sample['selectedsample'] = 'Ναι'
                                dataToSample.drop('selectedsample', inplace=True, axis=1)
                                sample = pd.merge(dataToSample, sample, on=["opsaa","name","final","amount"], how="left")
                                sample.fillna(" ", inplace=True)
                                sample = sample[["name", "opsaa", "selectedsample","final","amount"]]
                                sample = pd.concat([sample, dataRest])
                                sample['username'] = username
                                sample['date'] = now
                                sample['opsaa'] = sample['opsaa'].astype(str)
                                sample = sample[["username","date","name","opsaa","selectedsample","final","amount"]]
                                sample.to_sql('m193sampling', con, if_exists='append',dtype={"username": sa.types.String(),
                                    "date": sa.types.String(), "opsaa": sa.types.String(),
                                    "amount": sa.types.String(), "final": sa.types.String(),
                                    "name": sa.types.String(), "selectedsample": sa.types.String()})
                                sample = sample.to_dict(orient='records')
                                engine.dispose()
                                self.send_response(200)
                                self.send_header('Content-type', 'application/json')
                                self.end_headers()
                                self.wfile.write(json.dumps(sample).encode())
                            else:
                                temperror = {"myerror": "Δεν έχετε επισυνάψει το αρχείο στη σωστή μορφή."}
                                self.send_response(404)
                                self.end_headers()
                                self.wfile.write(json.dumps(temperror).encode())
                else:
                    temperror = {"myerror": "Οι στήλες του αρχείου δεν είναι σωστές."}
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(json.dumps(temperror).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
        
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def getHistorySampleM193(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()
    
            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # SQL query to check if the user has permission
            get_user = """SELECT * FROM public.tools_rights WHERE m193sampling=1 AND username=(%(username)s)"""
            users = pd.read_sql_query(get_user, con=engine, params={"username": username})
    
            # If user has permission, fetch history, else return error
            if len(users) > 0:
                con.execute(
                    'INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today)
                )
                get_history = """SELECT * FROM public.m193sampling"""
                history = pd.read_sql_query(get_history, con=engine)
                history = pd.DataFrame(history.groupby("date", as_index=False)["opsaa"].count(), index=None)
                history = history.to_dict(orient='records')
    
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(history).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
    
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode('utf-8')
            self.wfile.write(error_message)

    
    def getSampleM193(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            code = query_params.get('code', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            get_user = """SELECT * FROM public.tools_rights WHERE m193sampling=1 AND username=(%(username)s)"""
            users = pd.read_sql_query(get_user, con=engine, params={"username": username})

            # If user exists, return their information, else return error
            if (len(users) > 0):
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                date = request.args.get('date')
                get_sample = """SELECT * FROM public.m193sampling WHERE date=(%(date)s)"""
                sample = pd.read_sql_query(get_sample, con=engine, params={"date": date})
                sample = sample.to_dict(orient='records')

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(sample).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def createSampleM16(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            code = query_params.get('code', [None])[0]
            today = datetime.date.today()
            now = datetime.datetime.now()
            now = now.strftime("%d/%m/%Y %H:%M:%S")

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # SQL query to get user info with correct parameterization using %s for psycopg2
            get_user_query = "SELECT * FROM public.tools_rights WHERE m16sampling=1 AND username=(%(username)s)"
            user_info = pd.read_sql_query(get_user_query, con=engine, params={"username": username})
    
            # If user exists, return their information, else return error
            if len(user_info) > 0:
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                get_data = """SELECT * FROM public.m16available WHERE code=(%(code)s)"""
                data = pd.read_sql_query(get_data, con=engine, params={"code": code})
                #data = data.sample(frac=1)
                data = data.iloc[np.random.permutation(data.index)].reset_index(drop=True)
                threshold = 0.1 * data['amount'].sum()
                sum = 0
                data['selectedsample'] = ' '
                for i in range(len(data)):
                    if ((sum<threshold) and (u"Έμμεσες" not in data['subcategory'][i])):
                        sum = sum + data['amount'][i]
                        data['selectedsample'][i] = 'Ναι'
                data.fillna(" ", inplace=True)
                data['username'] = username
                data['date'] = now
                data['code'] = code
                sample = data[["username", "date", "pske", "code", "name", "subcategory", "amount", "expenseaa", "invoicenumber", "selectedsample"]]
                total = sample['amount'].astype('float').sum()
                totalsample = sample.loc[sample['selectedsample'] != ' ', 'amount'].astype('float').sum()
                sample.to_sql('m16sampling', con, if_exists='append', dtype={"username": sa.types.String(),
                "date": sa.types.String(),"pske": sa.types.String(),"name": sa.types.String(),
                "amount": sa.types.String(),"subcategory": sa.types.String(),
                "code": sa.types.String(),"selectedsample": sa.types.String()})
                sample = sample.to_dict(orient='records')
                # Create the JSON structure
                response_data = {
                    "sample": sample,
                    "total": total,
                    "totalsample": totalsample
                }
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def getAvailablePaymentsM16(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # Combine the two queries into one
            if (username=='itsergoulas@mou.gr'):
                get_user = """SELECT * FROM public.tools_rights WHERE username=(%(username)s)"""
            else:
                get_user = """SELECT * FROM public.tools_rights WHERE m16sampling=1 AND username=(%(username)s)"""
            users = pd.read_sql_query(get_user, con=engine, params={"username": username})

            # If user exists, return their information, else return error
            if (len(users) > 0):
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                sender_email = "itsergoulas@mou.gr"
                #sender_email = "agristatseu@gmail.com"
                m = imaplib.IMAP4_SSL("imap.gmail.com") # Connecting to the gmail imap server
                #m.login("eydreports@gmail.com", "fmobgkhuxynbdkia")
                m.login("eydreports@gmail.com", "hsjojpqsmowilalt")
                m.select() # Select the mailbox
                resp, items = m.search(None, 'FROM', '"%s"' % sender_email)
                items = items[0].split()
                for i in range(1):
                    resp, data = m.fetch(items[len(items)-1], "(RFC822)")
                    email_body = data[0][1]
                    mail = email.message_from_string(email_body)
                    if mail.get_content_maintype() != 'multipart':
                        continue
                    for part in mail.walk():
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if part.get('Content-Disposition') is None:
                            continue
                        text = part.get_payload(decode=True)
                        text = text.replace('","', ';')
                        text = text.replace('\r', '')
                        text = text.replace('"', '')
                        available = pd.DataFrame([x.split(';') for x in text.split('\n')])
                        available.columns = available.iloc[0]
                        available = available[1:-1]
                        available = available.iloc[:, :11]
                    separator = available.index[available['Κωδικός Πρότασης'] == 'Κωδικός Πρότασης'].tolist()
                    #available = available.iloc[:, :-1]
                    available1 = available.iloc[:separator[0] - 1, :]
                    available2 = available.iloc[separator[0] - 1:, :]
                    available2 = available2.iloc[:, :-4]
                    available2 = available2.rename(columns=available2.iloc[0]).loc[1:]
                    available1 = available1[available1['Κωδικός Πρότασης'].notnull()]
                    available2 = available2[available2['Κωδικός Πρότασης'].notnull()]
                    available = pd.merge(available1, available2, on=['Κωδικός Πρότασης', 'Κωδικός Ενέργειας', 'ID Δαπάνης'])
                    available['Κωδικός Πρότασης'] = available['Κωδικός Πρότασης'].apply(lambda x: x.strip())
                    available.rename(columns={'Κωδικός Πρότασης': 'pske', 'Κωδικός Ενέργειας': 'code',
                    'ID Δαπάνης': 'expenseid','ΑΑ Υλοπ. Δαπάνης': 'expenseaa',
                    'Αρ. Παραστατικού':'invoicenumber',
                    'Ημ/νία Έκδοσης':'invoicedate','ΑΦΜ Προμηθευτή':'invoiceafm',
                    'Κατάσταση Πρότασης': 'status', 'Επωνυμία': 'name',
                    'Υποκατηγορία Δαπάνης': 'subcategory', 'Συνολικό Ποσό': 'amount'}, inplace=True)
                    available['invoicenumber'] = available['invoicenumber']+" - "+available['invoicedate']+" - "+available['invoiceafm']
                    available['amount'] = available['amount'].astype('str')
                    available['amount'] = available['amount'].str.replace(',', '.')
                    get_sampled = """SELECT DISTINCT code FROM public.m16sampling"""
                    sampled = pd.read_sql_query(get_sampled, con=engine)
                    available = available[~available['code'].isin(sampled['code'])].dropna()
                    available.to_sql('m16available', con, if_exists='replace', dtype={"pske": sa.types.String(),
                                   "code": sa.types.String(), "status": sa.types.String(), "expenseaa": sa.types.String(),
                                    "name": sa.types.String(), "subcategory": sa.types.String(),"invoicenumber": sa.types.String(),
                                    "amount": sa.types.Float(precision=3, asdecimal=True)})
                    availableUnique = available[['pske','code','status']].drop_duplicates(keep='first')
                    if (username!='itsergoulas@mou.gr'):
                        get_user = """SELECT * 
                                    FROM (SELECT "Κωδικός Πρότασης", "Υπηρεσία" FROM public.m16projects) AS t1 
                                    JOIN (SELECT foreas FROM public.tools_rights WHERE username = %(username)s) AS t2 
                                    ON t1."Υπηρεσία" = t2.foreas;
                                    """
                        users = pd.read_sql_query(get_user, con=engine, params={"username": username})
                        users[u'Κωδικός Πρότασης'] = users[u'Κωδικός Πρότασης'].apply(lambda x: x.encode('UTF-8'))
                        availableUnique = pd.merge(availableUnique,users,left_on="pske",right_on=u"Κωδικός Πρότασης")
                    availableUnique = availableUnique.to_dict(orient='records')

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(availableUnique).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def getHistorySampleM16(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # Combine the two queries into one
            insert_history_query = """
                INSERT INTO public.tools_history (username, userid, mydate)
                SELECT
                    %(username)s AS username,
                    %(userid)s AS userid,
                    %(today)s AS today
                WHERE EXISTS (
                    SELECT 1
                    FROM public.tools_rights
                    WHERE m16sampling = 1 AND username = %(username)s
                );
            """
            try:
                con.execute(insert_history_query, {"username": username, "userid": userid, "today": today})
                get_history_query = """SELECT DISTINCT pske, code FROM public.m16sampling"""
                history = pd.read_sql_query(get_history_query, con=engine)
                if (username != 'itsergoulas@mou.gr'):
                    get_user = """SELECT * 
                                FROM (SELECT "Κωδικός Πρότασης", "Υπηρεσία" FROM public.m16projects) AS t1 
                                JOIN (SELECT foreas FROM public.tools_rights WHERE username = %(username)s) AS t2 
                                ON t1."Υπηρεσία" = t2.foreas;
                                """
                    users = pd.read_sql_query(get_user, con=engine, params={"username": username})
                    users[u'Κωδικός Πρότασης'] = users[u'Κωδικός Πρότασης'].apply(lambda x: x.encode('UTF-8'))
                    history['pske'] = history['pske'].apply(lambda x: x.encode('UTF-8'))
                    history = pd.merge(history, users, left_on="pske", right_on=u"Κωδικός Πρότασης")
                    history = history.to_dict(orient='records')
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(history).encode())
            except sa.exc.IntegrityError:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def getSampleM16(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            code = query_params.get('code', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # Combine the two queries into one
            combined_query = """
                WITH inserted_history AS (
                    INSERT INTO public.tools_history (username, userid, mydate)
                    SELECT
                        %(username)s AS username,
                        %(userid)s AS userid,
                        %(today)s AS today
                    WHERE EXISTS (
                        SELECT 1
                        FROM public.tools_rights
                        WHERE m16sampling = 1 AND username = %(username)s
                    )
                    RETURNING *
                )
                SELECT
                    m16sampling.*,
                    th.username,
                    th.userid,
                    th.mydate
                FROM public.m16sampling
                LEFT JOIN inserted_history th ON 1 = 1
                WHERE code = %(code)s;
            """
    
            # If user exists, return their information, else return error
            result = pd.read_sql_query(combined_query, con=engine, params={"username": username, "userid": userid, "today": today, "code": code})
            if not result.empty:
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                total = result['amount'].astype('float').sum()
                totalsample = result.loc[result['selectedsample'] != ' ', 'amount'].astype('float').sum()
                sample = result.to_dict(orient='records')
                # Create the JSON structure
                response_data = {
                    "sample": sample,
                    "total": total,
                    "totalsample": totalsample
                }

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def getReports(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # SQL query to get user info with correct parameterization using %s for psycopg2
            get_user_query = "SELECT * FROM public.tools_rights WHERE reports=1 AND username=(%(username)s)"
            user_info = pd.read_sql_query(get_user_query, con=engine, params={"username": username})
    
            # If user exists, return their information, else return error
            if len(user_info) > 0:
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                get_reports = """SELECT reportid, reportname FROM public.reports"""
                reports = pd.read_sql_query(get_reports, con=engine)
                reports = reports.to_dict(orient='records')

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(reports).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)

    def getReport(self, query_params):
        try:
            # Get parameters from the query
            username = query_params.get('username', [None])[0]
            userid = query_params.get('userid', [None])[0]
            type = query_params.get('type', [None])[0]
            today = datetime.date.today()

            if not username or not userid:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing username or userid")
                return
    
            # Connect to the database
            engine = sa.create_engine(DATABASE_URL, encoding='utf-8')
            con = engine.connect()
    
            # SQL query to get user info with correct parameterization using %s for psycopg2
            get_user_query = "SELECT * FROM public.tools_rights WHERE reports=1 AND username=(%(username)s)"
            user_info = pd.read_sql_query(get_user_query, con=engine, params={"username": username})
    
            # If user exists, return their information, else return error
            if len(user_info) > 0:
                con.execute('INSERT INTO public.tools_history VALUES (\'%s\',\'%s\',\'%s\')' % (username, userid, today))
                reportid = request.args.get('reportid')
                get_report = """SELECT * FROM public.reports WHERE reportid=(%(reportid)s)"""
                reportquery = pd.read_sql_query(get_report, con=engine, params={"reportid": reportid})
                report = pd.read_sql_query(reportquery['reportquery'][0], con=engine, params={"year": "2024%"})
                report = report.append(report.sum(numeric_only=True), ignore_index=True)
                report = report.replace(np.nan, "")
                print(report.columns)
                if (type=="pdf"):
                    df_num = report.select_dtypes(include=[np.float])
                    mycolumns = df_num.columns
                    for column in mycolumns:
                        report[column] = report[column].map('{:,.2f}'.format).str.replace(",", "~").str.replace(".", ",").str.replace("~", ".")
                #dd = OrderedDict()
                print(report.columns)
                report = report.to_dict(orient='records', into=OrderedDict)
                # Create the JSON structure
                response_data = {
                    "report": reports_dict,
                    "reportname": report_name
                }
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps(myerror).encode())
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def do_POST(self):
        # Handle POST requests if necessary
        pass

def run(server_class=HTTPServer, handler_class=MyRequestHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Server running on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
