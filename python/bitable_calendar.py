# -*- coding: UTF-8 -*-
import api
import config
import logging
from datetime import datetime
import utils
from mock import schema, raw_data, name_map
import os, datetime, sys, time
from odps import ODPS
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.ERROR)

import os
logging.info(os.getcwd())

def rgb(r: int, g: int, b: int): return (r<<16) + (g<<8) + b


def sync_records_to_calendar(client: api.Client, access_token, calendar_id, app_token, table_id):
    '''generate calendar events from bitable records'''

    records = client.get_records(access_token, app_token, table_id)
    for record in records:
        try:
            fields = record.get('fields')
            version = fields.get(config.KEY_VERSION)
            proposal_date = datetime.fromtimestamp(fields.get(config.KEY_STARTUP) / 1000)
            delivery_date = datetime.fromtimestamp(fields.get(config.KEY_SUBMIT) / 1000)
            expected_online_time = datetime.fromtimestamp(fields.get(config.KEY_GREY) / 1000)
            online_time = datetime.fromtimestamp(fields.get(config.KEY_GA) / 1000)
            print(version, proposal_date, delivery_date, expected_online_time, online_time)
            print(client.create_event(access_token, calendar_id, start_time=proposal_date, end_time=delivery_date, summary=config.FORMATTER_DEV.format(version), color=rgb(221, 88, 24)))
            print(client.create_event(access_token, calendar_id, start_time=delivery_date, end_time=expected_online_time, summary=config.FORMATTER_TEST.format(version), color=rgb(85, 220, 101)))
            print(client.create_event(access_token, calendar_id, start_time=expected_online_time, end_time=online_time, summary=config.FORMATTER_GREY.format(version), color=rgb(118, 244, 244)))
        except TypeError:
            pass



def write_bitable(client: api.Client, access_token, raw_data, schema, name_map) -> str:
    '''write project release date info to bitable'''
    table_id = client.create_table(access_token, config.APP_TOKEN, config.TABLE_NAME)
    resp = client.get_fields_list(access_token, config.APP_TOKEN, table_id)
    current_fields = resp['items']

    for index, (field_name, field_type) in enumerate(schema.items()):
        try:
            if index >= len(current_fields):
                print(client.add_field(access_token, config.APP_TOKEN, table_id, name_map[field_name], field_type))
            else:
                print(client.update_field(
                    access_token, 
                    config.APP_TOKEN, 
                    table_id, 
                    current_fields[index]['field_id'], 
                    name_map[field_name],
                    field_type))
        except utils.LarkException as e:
            if e.code == 1254606: # DataNotChange
                pass
            else:
                raise

    resp = client.get_records_list(access_token, config.APP_TOKEN, table_id)
    current_records = resp['items']
    if current_records is None:
        current_records = []

    updated_records = []
    created_records = []

    for index, version_info in enumerate(raw_data):
        fields = {}
        for field_name, field_value in version_info.items():
            if schema[field_name] == 5:
                field_value = datetime.strptime(field_value, '%Y-%m-%d').timestamp() * 1000
            fields[name_map[field_name]] = field_value
        if index >= len(current_records):
            created_records.append({'fields': fields})
        else:
            updated_records.append({'record_id': current_records[index]['record_id'], 'fields': fields})
    if updated_records:
        resp = client.batch_update_records(access_token, config.APP_TOKEN, table_id, updated_records)
    if created_records:
        resp = client.batch_create_records(access_token, config.APP_TOKEN, table_id, created_records)
    return table_id

def bitable_to_calendar():
    '''write project release date info to bitable and sync it to calendar'''
    # init api client
    client = api.Client(config.LARK_HOST)

    # get tenant access token        
    access_token = client.get_tenant_access_token(config.APP_ID, config.APP_SECRET)

    # create a new table and push local json-like version iteration data to it
    table_id = write_bitable(client, access_token, raw_data, schema, name_map)

    # create new calendar and get its id
    calendar = client.create_calendar(access_token, permissions='public', summary=config.SUMMARY)
    print(calendar)
    calendar_id = calendar['calendar_id']

    # sync records to calendar
    sync_records_to_calendar(client, access_token, calendar_id, config.APP_TOKEN, table_id)

if __name__ == "__main__":
    
    client = api.Client(config.LARK_HOST)
    access_token = client.get_tenant_access_token(config.APP_ID,config.APP_SECRET)
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-1)
    pt = yes_time.strftime('%Y%m%d')
    recordList=client.get_records_list(access_token, config.APP_TOKEN,config.TABLE_ID,config.VIEW_ID)
    dataList=[]
    
    for i in recordList['items']:
        #  print(i)
         datamap={}
         datamap['autoencoding_id']=i['fields']['自动编码']
         datamap['merchant_id']=i['fields']['商户ID']
         if i['fields']['合伙人名称']:
             datamap['partner_name']=i['fields']['合伙人名称'][0]['text']
         else:
            datamap['partner_name']=None
         datamap['partner_firm']=i['fields']['合伙人公司名称']
         datamap['intention_id']=i['fields']['意向ID']
         if (i['fields']['城市']):
             datamap['city']=i['fields']['城市'][0]['text']
         else:
            datamap['city']=None
         datamap['store_code']=i['fields']['门店编码'][0]
         datamap['store_id']=i['fields']['门店ID'][0]['text']
         datamap['store_name']=i['fields']['门店名称'][0]['text']
         datamap['intent_gold']=i['fields']['意向金']
         if i['fields']['意向金收款渠道']:
             datamap['intent_source']=i['fields']['意向金收款渠道'][0]
         else:
            datamap['intent_source']=None
         if i['fields']['应收保证金']:
            datamap['margin_receivables']=i['fields']['应收保证金'][0]
         else:
            datamap['margin_receivables']=None
         if i['fields']['意向金转入金额']:
            datamap['intention_amount_to']=i['fields']['意向金转入金额'][0]['text']
         else:
            datamap['intention_amount_to']=None
         if i['fields']['保证金收款渠道']:
            datamap['margin_channels']=i['fields']['保证金收款渠道'][0]
         else:
            datamap['margin_channels']=None
         datamap['landing_date']=i['fields']['落地日期']
         if i['fields']['收款渠道是否一致']:
            datamap['collection_same']=i['fields']['收款渠道是否一致'][0]['text']
         else:
            datamap['collection_same']=None
         if i['fields']['应收加盟费']:
            datamap['receivable_franchise_fes']=i['fields']['应收加盟费'][0]
         else:
            datamap['receivable_franchise_fes']=None
         if i['fields']['加盟费类型']:
            datamap['franchise_type']=i['fields']['加盟费类型'][0]
         else:
            datamap['franchise_type']=None
         if i['fields']['加盟费']:
            datamap['franchise_fes']=i['fields']['加盟费'][0]
         else:
            datamap['franchise_fes']=None
         if i['fields']['分期期数']:
            datamap['instalments']=i['fields']['分期期数'][0]
         else:
            datamap['instalments']=None
         datamap['amount_per']=i['fields']['每期金额'][0]['text']
         if i['fields']['应收设计费']:
            datamap['receivable_design_fees']=i['fields']['应收设计费'][0]
         else:
            datamap['receivable_design_fees']=None
         datamap['fact_design_fees']=i['fields']['设计费实收金额']
         datamap['diff_design_fees']=i['fields']['设计费差值']
         if i['fields']['应收器械款']:
            datamap['receivable_machine_fees']=i['fields']['应收器械款'][0]
         else:
            datamap['receivable_machine_fees']=None
         datamap['fact_machine_fees']=i['fields']['器械款实收金额']
         datamap['diff_machine_fees']=i['fields']['器械费差值']
         datamap['receivable_marketing_materials']=i['fields']['应收装修及营销物料']
         if '是否退出' in i['fields'].keys():
             datamap['is_exits']=i['fields']['是否退出']
         else:
             datamap['is_exits']=None
         if '成本项' in i['fields'].keys():
            datamap['cost_item']=i['fields']['成本项']
         else:
            datamap['cost_item']=None
         datamap['creator']=i['fields']['创建人']['name']
         datamap['create_time']=i['fields']['创建时间']
         datamap['updateor']=i['fields']['最后修改人']['name']
         datamap['update_time']=i['fields']['最后修改时间']
         dataList.append(datamap)
    # print(dataList)
    dataToOdpsList=[]
    metakeys=[]
    for i  in dataList:
        dataToOdpsList.append(list(i.values()))
        metakeys=i.keys()
    print(dataToOdpsList)
    print(metakeys)
    odps = ODPS('xxxx', 'xxxxxx', 'xxxx')
    t = odps.get_table('xxxxx')
    with t.open_writer(partition='pt={pt}'.format(pt=pt),create_partition=True) as writer:
            writer.write(dataToOdpsList)