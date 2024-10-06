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

    def handle_get_user_info(self, query_params):
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
            user_info = pd.read_sql_query(get_user_query, con=engine, params=[username])
    
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
                self.wfile.write(b"No access rights.")
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
