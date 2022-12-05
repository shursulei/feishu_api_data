# -*- coding: UTF-8 -*-
import datetime
from typing import List
from utils import request
import sys
if sys.version_info.minor >= 8:
    from typing import Literal
else:
    from typing_extensions import Literal


class Client(object):
    def __init__(self, lark_host):
        self._host = lark_host

    def get_tenant_access_token(self, app_id, app_secret):
        url = self._host+"/open-apis/auth/v3/app_access_token/internal/"
        headers = {
            'Content-Type': 'application/json; charset=utf-8'
        }
        payload = {
            'app_id': app_id,
            'app_secret': app_secret
        }
        resp = request("POST", url, headers, payload)
        return resp['tenant_access_token']

    def get_records(self, access_token: str, app_token: str, table_id: str, view_id: str=None):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        
        params = {}
        if view_id: params['view_id'] = view_id
        resp = request("GET", url, headers, params=params)
        return resp['data']['items']


    def create_calendar(
        self, 
        access_token: str, 
        summary: str=None, 
        description: str=None, 
        permissions: Literal["private", "show_only_free_busy", "public"]=None,
        color: int=None,
        summary_alias: str=None):
        url = f"{self._host}/open-apis/calendar/v4/calendars"

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {}
        if summary: payload['summary'] = summary
        if description: payload['description'] = description
        if permissions: payload['permissions'] = permissions
        if color: payload['color'] = color
        if summary_alias: payload['summary_alias'] = summary_alias

        resp = request("POST", url, headers, payload)
        return resp['data']['calendar']

    def create_event(
        self,
        access_token: str,
        calendar_id: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        vchat=None,
        location=None,
        reminders=None,
        schemas=None,
        summary: str=None,
        description: str=None,
        need_notification: bool=None,
        visibility: Literal['default', 'public', 'private']=None,
        attendee_ability: Literal['none', 'can_see_others', 'can_invite_others', 'can_modify_event']=None,
        free_busy_status: Literal['busy', 'free']=None,
        color: int=None,
        recurrence: str=None,
        ):
        url = f"{self._host}/open-apis/calendar/v4/calendars/{calendar_id}/events"

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {
            'start_time': {'timestamp': int(start_time.timestamp())},
            'end_time': {'timestamp': int(end_time.timestamp())}
        }
        if vchat: payload['vchat'] = vchat
        if location: payload['location'] = location
        if reminders: payload['reminders'] = reminders
        if schemas: payload['schemas'] = schemas
        if summary: payload['summary'] = summary
        if description: payload['description'] = description
        if need_notification: payload['need_notification'] = need_notification
        if visibility: payload['visibility'] = visibility
        if attendee_ability: payload['attendee_ability'] = attendee_ability
        if free_busy_status: payload['free_busy_status'] = free_busy_status
        if color: payload['color'] = color
        if recurrence: payload['recurrence'] = recurrence
        resp = request("POST", url, headers, payload)
        return resp['data']['event']

    def batch_create_records(self, access_token: str, app_token: str, table_id: str, records: List[dict]):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {
            'records': records
        }
        resp = request("POST", url, headers, payload)
        return resp['data']['records']

    def get_fields_list(self, access_token: str, app_token: str, table_id: str, view_id: str=None, page_token: str=None, page_size: int=None):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        params = {}
        if view_id: params['view_id'] = view_id
        if page_token: params['page_token'] = page_token
        if page_size: params['page_size'] = page_size
        resp = request("GET", url, headers, params=params)
        return resp['data']

    def update_field(self, access_token: str, app_token: str, table_id: str, field_id: str, field_name: str, type: int, property: dict=None):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {
            'field_name': field_name,
            'type': type,
        }
        if property: payload['property'] = property
        resp = request("PUT", url, headers, payload)
        return resp['data']['field']

    def get_records_list(self, access_token: str, app_token: str, table_id: str, view_id: str=None, page_token: str=None, page_size: int=None):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        params = {}
        if view_id: params['view_id'] = view_id
        if page_token: params['page_token'] = page_token
        if page_size: params['page_size'] = page_size
        resp = request("GET", url, headers, params=params)
        return resp['data']
        

    def batch_update_records(self, access_token: str, app_token: str, table_id: str, records: List[dict], user_id_type: str=None):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {
            'records': records
        }
        resp = request("POST", url, headers, payload)
        return resp['data']['records']

    def create_table(self, access_token: str, app_token: str, table_name: str=None) -> str:
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {}
        if table_name: payload['table'] = {'name': table_name}
        resp = request("POST", url, headers, payload)
        return resp['data']['table_id']

    def add_field(self, access_token: str, app_token: str, table_id: str, field_name: str, type: int, property: dict=None):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {
            'field_name': field_name,
            'type': type
        }
        if property: payload['property'] = property
        resp = request("POST", url, headers, payload)
        return resp['data']['field']

    def batch_create_records(self, access_token: str, app_token: str, table_id: str, records: List[dict]):
        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+access_token,
        }
        payload = {
            'records': records
        }
        resp = request("POST", url, headers, payload)
        return resp['data']['records']




