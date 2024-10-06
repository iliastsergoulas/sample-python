from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib
import json
import sqlalchemy as sa
import pandas as pd
import datetime
import os

# Get the database URL from the environment (as required by DigitalOcean App Platform)
DATABASE_URL = os.getenv('DATABASE_URL')

class MyRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        # Parse URL and parameters
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)

        # Determine the endpoint
        if parsed_path.path == '/getUserInfo':
            self.handle_get_user_info(query_params)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Endpoint not found.")
    
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
                self.wfile.write("Δεν έχετε δικαίωμα πρόσβασης.".encode('utf-8'))
            engine.dispose()
    
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            error_message = f"Internal server error: {e}".encode()
            self.wfile.write(error_message)
    
    def get_reports(self, query_params):
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
                self.wfile.write("Δεν έχετε δικαίωμα πρόσβασης.".encode('utf-8'))
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
                self.wfile.write("Δεν έχετε δικαίωμα πρόσβασης.".encode('utf-8'))
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
