"""BoilingData WebSockets Client module"""
import os
import websockets
import boto3
import botocore.auth
from botocore.exceptions import NoCredentialsError
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

# Preview environment in eu-west-1
AWS_REGION = "eu-west-1"
USER_POOL_ID = "eu-west-1_0GLV9KO1p"
CLIENT_ID = "37f44ql7bp5p8fpk5qrh2sgu8"
BOILING_WSS_URL = "wss://4rpyi2ae3f.execute-api.eu-west-1.amazonaws.com/prodbd/"
IDP_URL = "cognito-idp.eu-west-1.amazonaws.com/eu-west-1_0GLV9KO1p"
IDENTITY_POOL_ID = "eu-west-1:bce21571-e3a6-47a4-8032-fd015213405f"


class BoilingData:
    """Create authenticated WebSocket connection to BoilingData"""

    def __init__(self, region=AWS_REGION):
        self.websocket = None
        self.aws_creds = None
        self.region = region
        self.id_client = boto3.client("cognito-identity")
        self.idp_client = boto3.client("cognito-idp")
        self.username = os.getenv("BD_USERNAME", "")
        self.password = os.getenv("BD_PASSWORD", "")
        if self.username == "" or self.password == "":
            raise ValueError(
                "Missing username (BD_USERNAME) and/or "
                + "password (BD_PASSWORD) environment variable(s)"
            )

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
        return request.headers

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

    async def connect(self):
        """Connect to BoilingData WebSocket API"""
        auth_headers = self._get_auth_headers()
        self.websocket = await websockets.connect(
            BOILING_WSS_URL, extra_headers=auth_headers
        )

    async def send(self, msg):
        """Send message to BoilingData"""
        return await self.websocket.send(msg)

    async def close(self):
        """Close WebSocket connection to Boiling"""
        return await self.websocket.close()

    async def recv(self):
        """Receive message from Boiling"""
        return await self.websocket.recv()
