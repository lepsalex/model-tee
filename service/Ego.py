import os
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv

load_dotenv()


class Ego:
    _ego_url = os.getenv("EGO_TOKEN_URL")
    _client_id = open(os.getenv("EGO_CLIENT_ID_FILE")).read()
    _client_secret = open(os.getenv("EGO_CLIENT_SECRET_FILE")).read()

    _ego_client = BackendApplicationClient(client_id=_client_secret)

    @staticmethod
    def getToken():
        oauth = OAuth2Session(client=Ego._ego_client)
        return oauth.fetch_token(token_url=Ego._ego_url,
                                 client_id=Ego._client_id,
                                 client_secret=Ego._client_secret)

    @staticmethod
    def getAuthHeader():
        return {
            'Authorization': 'Bearer ' + Ego.getToken()
        }
