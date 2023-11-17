"""BoilingData Client"""
import os, json, uuid, time
import rel
import duckdb
import threading
import websocket
import boto3
import asyncio
import botocore.auth
from pprint import pprint
from botocore.exceptions import NoCredentialsError
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from py_boilingdata.data_queue import DataQueue


# Preview environment in eu-west-1
# TODO: Put in dotenv for example
AWS_REGION = "eu-west-1"
USER_POOL_ID = "eu-west-1_0GLV9KO1p"
CLIENT_ID = "37f44ql7bp5p8fpk5qrh2sgu8"
# BOILING_WSS_URL = "wss://4rpyi2ae3f.execute-api.eu-west-1.amazonaws.com/prodbd/"
BOILING_WSS_URL = "wss://e4f3t7fs58.execute-api.eu-west-1.amazonaws.com/devbd/"
IDP_URL = "cognito-idp.eu-west-1.amazonaws.com/eu-west-1_0GLV9KO1p"
IDENTITY_POOL_ID = "eu-west-1:bce21571-e3a6-47a4-8032-fd015213405f"
BOILING_SEARCH_TERMS = [
    "'s3://",
    "glue('",
    "glue ('",
    "list('",
    "list ('",
    "share('",
    "share ('",
    "boilingdata",
    "boilingshares",
]


class BoilingData:
    """Run SQL with BoilingData and local DuckDB"""

    def __init__(self):
        self.bd_conn = BoilingDataConnection()
        self.conn = duckdb.connect(":memory:")

    async def populate(self):
        self.conn.execute("ATTACH ':memory:' AS boilingdata;")
        self.conn.execute("SET search_path='memory,boilingdata';")
        # Boiling specific "information_schema" table
        q = "SELECT * FROM information_schema.create_tables_statements"

        def cb(bd_tables):
            if bd_tables:
                for table in bd_tables:
                    self.conn.execute(table)

        await self.bd_conn.bd_execute(q, cb)

    def _is_boiling_execute(self, sql):
        ## 1) Get all Boiling tables so we know what to intercept
        q = """
            SELECT table_schema, table_name 
              FROM information_schema.tables 
             WHERE table_catalog = 'boilingdata';
            """
        boiling_tables = self.conn.execute(q).fetchall()
        for table in boiling_tables:
            if (
                sql
                and table[0] in sql
                and table[1] in sql
                and "SELECT column_name, data_type AS column_type, is_nullable AS null"
                not in sql
            ):
                return True
        ## 2) static words
        __sql = sql.lower().replace('"', "")
        if (
            not sql.lower().startswith("prepare ")
            and not "information_schema" in sql.lower()
            and not "SHOW catalogs like" in sql
            and any(term in __sql for term in BOILING_SEARCH_TERMS)
        ):
            return True
        return False

    ##
    ## public
    ##

    async def connect(self):
        """Connect to BoilingData"""
        await self.bd_conn.connect()

    async def close(self):
        """Close WebSocket connection to Boiling"""
        await self.bd_conn.close()

    async def execute(self, sql, cb):
        """Send SQL Query to Boiling or run locally"""
        if not self._is_boiling_execute(sql):
            return self.conn.execute(sql).fetchall()
        return await self.bd_conn.bd_execute(sql, cb)


class BoilingDataConnection:
    """Create authenticated WebSocket connection to BoilingData"""

    def __init__(self, region=AWS_REGION):
        self.region = region
        self.username = os.getenv("BD_USERNAME", "")
        self.password = os.getenv("BD_PASSWORD", "")
        if self.username == "" or self.password == "":
            raise ValueError(
                "Missing username (BD_USERNAME) and/or "
                + "password (BD_PASSWORD) environment variable(s)"
            )
        self.websocket = None
        self.aws_creds = None
        self.ws_app = None
        self.ws_trace = False
        self.bd_is_open = False
        self.id_client = boto3.client("cognito-identity")
        self.idp_client = boto3.client("cognito-idp")
        self.requests = dict()

    def _get_auth_headers(self):
        """
        Uses Cognito based AWS Credentials to sign BoilingData WebSocket URL.

        returns:
            authentication headers
        """
        credentials = self._get_credentials()
        request = AWSRequest(method="GET", url=BOILING_WSS_URL)
        signer = botocore.auth.SigV4Auth(credentials, "execute-api", self.region)
        signer.add_auth(request)
        headers = dict()
        for key, value in request.headers.items():
            headers[key] = value
        return headers

    def _get_cognito_tokens(self, username, password):
        try:
            response = self.idp_client.admin_initiate_auth(
                UserPoolId=USER_POOL_ID,
                ClientId=CLIENT_ID,
                AuthFlow="ADMIN_USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": password},
            )
            return response["AuthenticationResult"]
        except self.idp_client.exceptions.NotAuthorizedException as e:
            print("The username or password is incorrect.")
            raise e
        except NoCredentialsError as e:
            print("Credentials not available.")
            raise e

    def _get_credentials(self):
        if self.aws_creds:
            return self.aws_creds
        tokens = self._get_cognito_tokens(self.username, self.password)
        id_response = self.id_client.get_id(
            IdentityPoolId=IDENTITY_POOL_ID, Logins={IDP_URL: tokens["IdToken"]}
        )
        cred_response = self.id_client.get_credentials_for_identity(
            IdentityId=id_response["IdentityId"], Logins={IDP_URL: tokens["IdToken"]}
        )
        # Create a Credentials object
        self.aws_creds = Credentials(
            cred_response["Credentials"]["AccessKeyId"],
            cred_response["Credentials"]["SecretKey"],
            cred_response["Credentials"]["SessionToken"],
        )
        return self.aws_creds

    async def _ws_send(self, msg):
        print(f"> {msg}")
        return self.ws_app.send(msg)

    def _on_open(self, ws_app):
        print("WS OPEN")
        self.bd_is_open = True

    def _on_msg(self, ws_app, data):
        print(f"< {data}")
        msg = json.loads(data)
        reqId = msg.get("requestId")
        if not reqId:
            return
        msg_type = msg.get("messageType")
        if msg_type != "DATA":
            return
        req = self.requests.get(reqId)
        if not req:
            raise Exception(f"Could not find request queue for '{reqId}'")
        q = req["q"]
        q.push(msg)
        if q.is_done():
            q.delete()
            del self.requests[reqId]

    def _on_error(self, ws_app, error):
        print(f"WS ERROR: {error}")

    def _on_close(self, ws_app, code, msg):
        print(f"WS CLOSE: {code} {msg}")

    ##
    ## public
    ##

    def _all_messages_received(self, event):
        requestId = event["requestId"]
        data = event["data"]
        cb = self.requests.get(requestId)
        cb["callback"](data)

    async def bd_execute(self, sql, cb):
        if self.bd_is_open is not True:
            raise Exception("No Boiling connection")
        reqId = uuid.uuid4().hex
        body = '{"sql":"' + sql + '","requestId":"' + reqId + '"}'
        self.requests[reqId] = {
            "q": DataQueue(reqId, self._all_messages_received),
            "reqId": reqId,
            "callback": cb,
        }
        await self._ws_send(body)

    async def connect(self):
        """Connect to BoilingData WebSocket API"""
        if self.websocket is not None:
            raise Exception("WebSocket already exists")
        self.websocket = websocket.WebSocket()
        websocket.enableTrace(self.ws_trace)
        auth_headers = self._get_auth_headers()
        self.ws_app = websocket.WebSocketApp(
            BOILING_WSS_URL,
            header=auth_headers,
            on_message=self._on_msg,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        wst = threading.Thread(target=self.ws_app.run_forever)
        wst.daemon = True
        wst.start()
        timeoutS = 1
        while self.bd_is_open is not True and timeoutS < 10:
            await asyncio.sleep(1)
            timeoutS = timeoutS + 1

    async def close(self):
        """Close WebSocket connection to Boiling"""
        if self.ws_app:
            self.ws_app.close()
        if self.websocket:
            self.websocket.close()
