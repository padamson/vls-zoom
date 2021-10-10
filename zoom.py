import time
from typing import Optional, Dict, Union, Any, List
from datetime import datetime

import requests
from authlib.jose import jwt
from requests import Response


class Zoom:
    def __init__(self, api_key: str, api_secret: str, user_id: str):
        self.datetime_format_string = "%Y-%m-%dT%H:%M:%SZ"
        self.api_key = api_key
        self.api_secret = api_secret
        self.user_id = user_id
        self.base_url = "https://api.zoom.us/v2"
        self.reports_url = f"{self.base_url}/report/meetings"
        self.meetings_url = f"{self.base_url}/meetings"
        self.user_url = f"{self.base_url}/users/{self.user_id}"
        self.jwt_token_exp = 1800
        self.jwt_token_algo = "HS256"


    def get_meeting_ids(self, datetime_window: List, jwt_token: bytes,
                        next_page_token: Optional[str] = None) -> List:
        url: str = f"{self.user_url}/meetings"
        query_params: Dict[str, Union[int, str]] = {"page_size": 300, "type": "scheduled"}
        if next_page_token:
            query_params.update({"next_page_token": next_page_token})

        r: Response = requests.get(url,
                                   headers={"Authorization": f"Bearer {jwt_token.decode('utf-8')}"},
                                   params=query_params)

        full_list_of_meetings: List[dict] = r.json().get("meetings")

        list_of_meetings: List = []

        for meeting in full_list_of_meetings:
            start_time = datetime.strptime(meeting['start_time'], 
                                           self.datetime_format_string)
            if (start_time >= datetime_window[0] and
                start_time <= datetime_window[1]):
                list_of_meetings.append(meeting)

        return list_of_meetings


    def get_meeting_participants(self, meeting_id: str, jwt_token: bytes,
                                 next_page_token: Optional[str] = None) -> Response:
        url: str = f"{self.reports_url}/{meeting_id}/participants"
        query_params: Dict[str, Union[int, str]] = {"page_size": 300}
        if next_page_token:
            query_params.update({"next_page_token": next_page_token})

        r: Response = requests.get(url,
                                   headers={"Authorization": f"Bearer {jwt_token.decode('utf-8')}"},
                                   params=query_params)

        return r
    
    def get_meeting_registrants(self, meeting_id: str, jwt_token: bytes,
                                 next_page_token: Optional[str] = None) -> Response:
        url: str = f"{self.meetings_url}/{meeting_id}/registrants"
        query_params: Dict[str, Union[int, str]] = {"page_size": 300}
        if next_page_token:
            query_params.update({"next_page_token": next_page_token})

        r: Response = requests.get(url,
                                   headers={"Authorization": f"Bearer {jwt_token.decode('utf-8')}"},
                                   params=query_params)

        return r

    def generate_jwt_token(self) -> bytes:
        iat = int(time.time())

        jwt_payload: Dict[str, Any] = {
            "aud": None,
            "iss": self.api_key,
            "exp": iat + self.jwt_token_exp,
            "iat": iat
        }

        header: Dict[str, str] = {"alg": self.jwt_token_algo}

        jwt_token: bytes = jwt.encode(header, jwt_payload, self.api_secret)

        return jwt_token
