import os
import pickle

from google.auth.transport.requests import Request
from googleapiclient.discovery import build


class GoogleOAuth:
    @staticmethod
    def get_service(api_name, api_version, token_path):
        credentials = GoogleOAuth.get_credentials(token_path)
        service = build(api_name, api_version, credentials=credentials)
        return service

    @staticmethod
    def get_credentials(token_path):
        if os.path.exists(token_path):
            with open(token_path, "rb") as token:
                credentials = pickle.load(token)

            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
        else:
            raise Exception("not exists token")

        """Auth code not used; proceed with pre-issued Access Token only.
        (else: auth code flow omitted)
            client_secrets = load_user_credentials()
            flow = Flow.from_client_secrets_file(
                secret_path,
                scopes=[API_SCOPE],
                redirect_uri='http://localhost')
            auth_url, _ = flow.authorization_url()
            print('Please go to this URL: {}\n'.format(auth_url))
            code = input('Enter the authorization code: ')
            flow.fetch_token(code=code)
            credentials = flow.credentials
        """

        with open(token_path, "wb") as token:
            pickle.dump(credentials, token)

        return credentials
