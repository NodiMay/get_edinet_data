import requests
import os
import config
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import time
import uuid
import tempfile
import re
import numpy as np
from shutil import copyfileobj
from zipfile import ZipFile
from src.common.logger import SimpleLogger
from src.utils.sql_utils import SqlUtils, DocumentListTable, SecuritiesReportTable, EdinetcodeTable
from io import BytesIO

class EdinetUtils:

    def __init__(self):
        self.logger = SimpleLogger(__class__.__name__)
        self.logger.info("EdinetUtils init")

    def get_data_from_edinet(self, url_path: str, params: dict) -> dict :
        EDINET_BASE_URL = config.EDINET_BASE_URL
        url = EDINET_BASE_URL.format(url_path=url_path)
        params['Subscription-Key'] = config.EDINET_KEY

        response = requests.get(url, params=params)
        return response
    
    def save_all_document_list(self, days: int = 3, doc_info_type=2):

        # 今日の日付
        end_date = datetime.now()
        current_unix_time = int(time.time())
        generated_uuid = uuid.uuid4().hex

        hdf5_key = f'document_list_{current_unix_time}_{generated_uuid}'

        self.logger.info("start: get_doc_list")

        self.logger.info(f"hdf5 key: {hdf5_key}")
        document_list = []
        for x in range(days):
            target_date = (end_date - timedelta(days=x)).strftime('%Y-%m-%d')
            params = {
                'date': target_date,
                'type': doc_info_type
            }
            response = self.get_data_from_edinet(config.EDINET_DOC_INFO_URL_PATH, params)
            if response.status_code == 200:

                document_list.append(response.json())
                if (x) % 10 == 0:
                    self.logger.info(f"get doc info executed days: {x + 1}, current target_date:{target_date}")
            else:
                self.logger.error(f"get doc info failed, current target_date:{target_date}")

        self.logger.info("end: get_doc_list")
        
        metadata = MetaData()

        document_list_table = Table('document_list_table', metadata,
                                    Column('docID', String, primary_key=True),
                                    Column('edinetCode', String),
                                    Column('secCode', String),
                                    Column('JCN', String),
                                    Column('filerName', String),
                                    Column('fundCode', String),
                                    Column('ordinanceCode', String),
                                    Column('formCode', String),
                                    Column('docTypeCode', String),
                                    Column('periodStart', String),
                                    Column('periodEnd', String),
                                    Column('submitDateTime', String),
                                    Column('docDescription', String),
                                    Column('issuerEdinetCode', String),
                                    Column('subjectEdinetCode', String),
                                    Column('subsidiaryEdinetCode', String),
                                    Column('currentReportReason', String),
                                    Column('parentDocID', String),
                                    Column('opeDateTime', String),
                                    Column('withdrawalStatus', String),
                                    Column('docInfoEditStatus', String),
                                    Column('disclosureStatus', String),
                                    Column('xbrlFlag', String),
                                    Column('pdfFlag', String),
                                    Column('attachDocFlag', String),
                                    Column('englishDocFlag', String),
                                    Column('csvFlag', String),
                                    Column('legalStatus', String),
                                    )


        engine = create_engine(f'sqlite:///{config.EDINET_DB}')
        metadata.drop_all(engine)
        metadata.create_all(engine)

        self.logger.info("start: modify df")
        document_list_df = pd.json_normalize(document_list, record_path=['results'])
        document_list_df.drop(columns=['seqNumber'], inplace=True)
        document_list_df.drop_duplicates(subset=['docID'], inplace=True)
        self.logger.info("end: modify df")
        self.logger.info("start: save to hdf5")
        document_list_df.to_hdf('data/edinet.h5', key=hdf5_key, format='table', mode='w')
        self.logger.info("end: save to hdf5")

        self.logger.info("start: save to db")
        document_list_df.to_sql('document_list_table', con=engine, if_exists='append', index=False)
        self.logger.info("end: save to db")


    def download_document(self, doc_id: str, edinet_code: str, download_type: int = 1) -> tuple:
        params = {
            'type': download_type
        }
        self.logger.info("start: download_document")
        self.logger.info(f"doc_id: {doc_id}")
        url_path = config.EDINET_DOC_URL_PATH.format(doc_id=doc_id)
        response = self.get_data_from_edinet(url_path=url_path, params=params)

        self.logger.info(f"status_code: {response.status_code}")
        file_path = None
        status_code = response.status_code
        if status_code == 200:
            file_path = os.path.join(config.DOWNLOAD_PATH, f"{edinet_code}_{doc_id}{config.DOWNLOAD_TYPES[download_type]}")
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
        
        self.logger.info("end: download_document")
        return (status_code, file_path, doc_id, edinet_code) 

    def get_doc_id_list(self, edinet_code: str, target_date_start: str, target_date_end: str, doc_types: list[str] = ["120"]) -> dict:
        
        self.logger.info("start: get_doc_id_list")

        database_url = f'sqlite:///{config.EDINET_DB}'
        manager = SqlUtils(database_url, DocumentListTable)

        select_conditions = {
            "submitDateTime": {"type": "date", "filter_type": "between", "start": target_date_start, "end": target_date_end},
            "docTypeCode": {"type": "string", "filter_type": "in", "values": doc_types}
        }
        if edinet_code != "all":
            select_conditions["edinetCode"] = {"type": "string", "filter_type": "eq", "value": edinet_code}
        
        query_response = manager.get_with_compound_conditions(**select_conditions)

        self.logger.info("end: get_doc_id_list")

        print([f"{document_list_table.docID},{document_list_table.edinetCode},{document_list_table.submitDateTime}" for document_list_table in query_response])

        return query_response

    def xbrl_parser(self, xbrl_file_path: str):
        pass

    def save_tag_to_db(self, file_path: str):
        self.logger.info("start: save_tag_to_db")
        column_name_mapping = {
            '様式ツリー-標準ラベル（日本語）': 'standardLabelTree',
            '詳細ツリー-標準ラベル（日本語）': 'detailedLabelTree',
            '冗長ラベル（日本語）': 'verboseLabelJp',
            '標準ラベル（英語）': 'standardLabelEn',
            '冗長ラベル（英語）': 'verboseLabelEn',
            '用途区分、財務諸表区分及び業種区分のラベル（日本語）': 'classificationLabelJp',
            '用途区分、財務諸表区分及び業種区分のラベル（英語）': 'classificationLabelEn',
            '名前空間プレフィックス': 'namespacePrefix',
            '要素名': 'elementName',
            'elementId': 'elementId',
            'type': 'type',
            'substitutionGroup': 'substitutionGroup',
            'periodType': 'periodType',
            'balance': 'balance',
            'abstract': 'abstract',
            'depth': 'depth',
            'documentationラベル（日本語）': 'documentationLabelJp',
            'documentationラベル（英語）': 'documentationLabelEn',
            '参照リンク': 'referenceLink',
            'Document Information': 'documentInformation',
            'parentElementName': 'parentElementName',
            'parentStandardLabelTree': 'parentStandardLabelTree',
            'parentDetailedLabelTree': 'parentDetailedLabelTree',
            'submitDateTime': 'submitDateTime'
        }
        tag_df = pd.read_excel(file_path, sheet_name="9", header=1)
        tag_df.dropna(how='all', subset=['要素名'], inplace=True)
        tag_df.dropna(how='all', inplace=True)
        tag_df.fillna(method='ffill', inplace=True)
        print(tag_df.columns)
    
        tag_df.rename(columns=column_name_mapping, inplace=True)
        tag_df['parentElementName'] = np.where(tag_df['depth'] == 0, tag_df['elementName'], np.nan)
        tag_df['parentStandardLabelTree'] = np.where(tag_df['depth'] == 0, tag_df['standardLabelTree'], np.nan)
        tag_df['parentDetailedLabelTree'] = np.where(tag_df['depth'] == 0, tag_df['detailedLabelTree'], np.nan)
        tag_df['parentElementName'].fillna(method='ffill', inplace=True)
        tag_df['parentStandardLabelTree'].fillna(method='ffill', inplace=True)
        tag_df['parentDetailedLabelTree'].fillna(method='ffill', inplace=True)
        tag_df['elementId'] = tag_df['namespacePrefix'] + ':' + tag_df['elementName']
        tag_df['submitDateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tag_df['classificationLabelJp'] = tag_df['classificationLabelJp'].str.replace('_x000D_', ' ')
        tag_df['classificationLabelJp'] = tag_df['classificationLabelJp'].replace(r'\s+|\\n', ' ', regex=True)
        tag_df['classificationLabelEn'] = tag_df['classificationLabelEn'].str.replace('_x000D_', ' ')
        tag_df['classificationLabelEn'] = tag_df['classificationLabelEn'].replace(r'\s+|\\n', ' ', regex=True)
        tag_df['classificationLabelJp'] = tag_df['classificationLabelJp'].str.replace('_x000D_', ' ')
        tag_df['classificationLabelJp'] = tag_df['classificationLabelJp'].replace(r'\s+|\\n', ' ', regex=True)
        tag_df['referenceLink'] = tag_df['referenceLink'].str.replace('_x000D_', ' ')
        tag_df['referenceLink'] = tag_df['referenceLink'].replace(r'\s+|\\n', ' ', regex=True)


        engine = create_engine(f'sqlite:///{config.EDINET_DB}')
        tag_df.to_sql('tag_table', con=engine, if_exists='replace', index=False)

        self.logger.info("end: save_tag_to_db")
    
    def save_account_tag_to_db(self, file_path: str):
        self.logger.info("start: save_account_tag_to_db")

        column_name_mapping = {
            '科目分類': 'accountClassification',
            'industry': 'industry',
            '標準ラベル（日本語）': 'standardLabel',
            '冗長ラベル（日本語）': 'verboseLabel',
            '標準ラベル（英語）': 'standardLabelEn',
            '冗長ラベル（英語）': 'verboseLabelEn',
            '用途区分、財務諸表区分及び業種区分のラベル（日本語）': 'classificationLabelJp',
            '用途区分、財務諸表区分及び業種区分のラベル（英語）': 'classificationLabelEn',
            '名前空間プレフィックス': 'namespacePrefix',
            '要素名': 'elementName',
            'type': 'type',
            'substitutionGroup': 'substitutionGroup',
            'periodType': 'periodType',
            'balance': 'balance',
            'abstract': 'abstract',
            'depth': 'depth',
            '参照リンク': 'referenceLink',
            'parentElementName': 'parentElementName',
            'parentStandardLabel': 'parentStandardLabel',
            'submitDateTime': 'submitDateTime'
        }
        
        account_type_list = [
            "貸借対照表　科目一覧",
            "損益計算書　科目一覧",
            "包括利益計算書　科目一覧",
            "株主資本等変動計算書　科目一覧",
            "キャッシュ・フロー計算書　科目一覧",
            "社員資本等変動計算書　科目一覧",
            "投資主資本等変動計算書　科目一覧",
            "純資産変動計算書　科目一覧",
            "純資産変動計算書　科目一覧",
            "損益及び剰余金計算書　科目一覧",
        ]

        account_xlsx = pd.ExcelFile(file_path, engine='openpyxl')

        skip_sheet_list = ['目次', '勘定科目リストについて']

        account_df_list = []
        for sheet_name in account_xlsx.sheet_names:
            if sheet_name in skip_sheet_list:
                continue
            
            _account_df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)
            _account_df = _account_df[~_account_df['科目分類'].isin(account_type_list + ['科目分類'] + [np.nan])]
            _account_df = _account_df[~_account_df['科目分類'].isin(account_type_list + ['科目分類'] + [np.nan])]
            _account_df['industry'] = sheet_name

            account_df_list.append(_account_df)

        account_df = pd.concat(account_df_list)
        account_df.rename(columns=column_name_mapping, inplace=True)
        account_df['parentElementName'] = np.where(account_df['depth'] == 0, account_df['elementName'], np.nan)
        account_df['parentStandardLabel'] = np.where(account_df['depth'] == 0, account_df['standardLabel'], np.nan)

        account_df.rename(columns=column_name_mapping, inplace=True)
        account_df['parentElementName'] = np.where(account_df['depth'] == 0, account_df['elementName'], np.nan)
        account_df['parentStandardLabel'] = np.where(account_df['depth'] == 0, account_df['standardLabel'], np.nan)
        account_df['parentElementName'].fillna(method='ffill', inplace=True)
        account_df['parentStandardLabel'].fillna(method='ffill', inplace=True)
        account_df['elementId'] = account_df['namespacePrefix'] + ':' + account_df['elementName']
        account_df['submitDateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        account_df['classificationLabelJp'] = account_df['classificationLabelJp'].str.replace('_x000D_', ' ')
        account_df['classificationLabelJp'] = account_df['classificationLabelJp'].replace(r'\s+|\\n', ' ', regex=True)
        account_df['classificationLabelEn'] = account_df['classificationLabelEn'].str.replace('_x000D_', ' ')
        account_df['classificationLabelEn'] = account_df['classificationLabelEn'].replace(r'\s+|\\n', ' ', regex=True)
        account_df['classificationLabelJp'] = account_df['classificationLabelJp'].str.replace('_x000D_', ' ')
        account_df['classificationLabelJp'] = account_df['classificationLabelJp'].replace(r'\s+|\\n', ' ', regex=True)
        account_df['referenceLink'] = account_df['referenceLink'].str.replace('_x000D_', ' ')
        account_df['referenceLink'] = account_df['referenceLink'].replace(r'\s+|\\n', ' ', regex=True)

        engine = create_engine(f'sqlite:///{config.EDINET_DB}')
        account_df.to_sql('account_tag_table', con=engine, if_exists='replace', index=False)

        self.logger.info("end: save_account_tag_to_db")
    
    def get_securities_report_by_edinet_code(self, edinet_code: str, target_date_start: str, target_date_end: str, doc_types: list[str] = ["120", "140", "160"], org_file_prefix_list: list[str] = ["jpcrp030000", "jpcrp040300", "jpcrp050000"]) -> pd.DataFrame:
        self.logger.info("start: get_securities_report_by_edinet_code")
        response = self.get_doc_id_list(edinet_code, target_date_start, target_date_end, doc_types)

        target_dfs = []
        with tempfile.TemporaryDirectory() as temp_dir:
            print(temp_dir)
            for document_list in response:
                self.logger.info(f"doc_id: {document_list.docID}")
                self.logger.info(f"edinet_code: {document_list.edinetCode}")
                self.logger.info(f"submit_date_time: {document_list.submitDateTime}")
                self.logger.info(f"doc_type: {document_list.docTypeCode}")
                self.logger.info(f"doc_description: {document_list.filerName}")
                doc_id = document_list.docID
                edinet_code = document_list.edinetCode
                doc_type_code = document_list.docTypeCode
                status_code, file_path, doc_id, edinet_code = self.download_document(doc_id=doc_id, edinet_code=edinet_code, download_type=5)
                if status_code == 200:
                    self.logger.info(f"file_path: {file_path}")
                    with ZipFile(file_path, 'r') as zip_ref:
                        for file_info in zip_ref.infolist():
                            print(file_info.filename)
                            # プレフィックスが一致するファイルを探す
                            file_path = file_info.filename
                            _, filename = os.path.split(file_info.filename)
                            is_finished = False
                            for org_file_prefix in org_file_prefix_list:
                                if filename.startswith(org_file_prefix):
                                    period = "full"
                                    # # ファイルの拡張子を保持
                                    if org_file_prefix == "jpcrp040300":
                                        splited_value = filename.split("-")
                                        period = splited_value[1]
                                    if org_file_prefix == "jpcrp050000":
                                        period = "half"

                                    file_dates = re.findall(r'\d{4}-\d{2}-\d{2}', filename)
                                    target_df = pd.read_csv(zip_ref.open(file_info), encoding='utf-16-le', sep='\t')
                                    target_df['fiscalYear'] = file_dates[0]
                                    target_df['submitDateTime'] = file_dates[1]
                                    target_df['docID'] = doc_id
                                    target_df['edinetCode'] = edinet_code
                                    target_df['docTypeCode'] = doc_type_code
                                    target_df['period'] = period
                                    target_df['filePrefix'] = org_file_prefix
                                    target_dfs.append(target_df)
                                    is_finished = True
                                    break
                            if is_finished:
                                break
        
        column_name_mapping = {
            'docID': 'docID',
            'edinetCode': 'edinetCode',
            'docTypeCode': 'docTypeCode',
            'fiscalYear': 'fiscalYear',
            'period': 'period',
            'filePrefix': 'filePrefix',
            '要素ID': 'elementId',
            '項目名': 'itemName',
            'コンテキストID': 'contextId',
            '相対年度': 'relativeFiscalYear',
            '連結・個別': 'consolidatedOrIndividual',
            '期間・時点': 'periodOrPointInTime',
            'ユニットID': 'unitId',
            '単位': 'unit',
            '値': 'value',
            'submitDateTime': 'submitDateTime'
        }


        combined_df = pd.concat(target_dfs, ignore_index=True)
        combined_df.rename(columns=column_name_mapping, inplace=True)
        combined_df = combined_df.drop_duplicates()
        combined_df = combined_df[column_name_mapping.values()]
        self.logger.info("end: get_securities_report_by_edinet_code")
        return combined_df


    def save_securities_report_to_db(self, combined_df: pd.DataFrame) -> None:
        self.logger.info("start: save_securities_report_to_db")
        engine = create_engine(f'sqlite:///{config.EDINET_DB}')
        SecuritiesReportTable.metadata.create_all(engine)
        existing_df = pd.read_sql_table('securities_report_table', con=engine)
        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['docID', 'edinetCode', 'elementId', 'contextId'], keep='first')
        final_df = combined_df[~combined_df.index.isin(existing_df.index)]
        print(final_df.head(10))

        final_df.to_sql('securities_report_table', con=engine, if_exists='append', index=False)

        self.logger.info("end: save_securities_report_to_db")

    def get_by_element_id(self, element_id: str, fiscal_year: str, edinet_id: str, period: list[str] = ["full", "half", "q1r", "q2r", "q3r"], doc_id: str = None, relative_fiscal_year: str = "当期") -> pd.DataFrame:
        self.logger.info("start: get_by_element_id")

        database_url = f'sqlite:///{config.EDINET_DB}'
        manager = SqlUtils(database_url, SecuritiesReportTable)
        select_conditions = {
            "elementId": {"type": "string", "filter_type": "eq", "value": element_id},
            "fiscalYear": {"type": "string", "filter_type": "eq", "value": fiscal_year},
            "edinetCode": {"type": "string", "filter_type": "eq", "value": edinet_id},
            "period": {"type": "string", "filter_type": "in", "values": period},
            "relativeFiscalYear": {"type": "string", "filter_type": "eq", "value": relative_fiscal_year}
        }
        if doc_id:
            select_conditions["docID"] = {"type": "string", "filter_type": "eq", "value": doc_id}
        
        self.logger.info(select_conditions)
        query_response = manager.get_with_compound_conditions(**select_conditions)
        print(len(query_response))
        
        column_names = [column.name for column in SecuritiesReportTable.__table__.columns]

        result_dict_list = []

        for securities_report_table in query_response:
            curr_result_dict = {}
            for column_name in column_names:
                curr_result_dict[column_name] = getattr(securities_report_table, column_name)
            if curr_result_dict:
                result_dict_list.append(curr_result_dict)
        
        result_df = pd.DataFrame(result_dict_list)
        self.logger.info(result_df.head(10))

        self.logger.info("end: get_by_element_id")

        return result_df
    
    def save_all_edinet_csv_doc_to_db(self, target_date_start: str, target_date_end: str, batch_size=100):
        self.logger.info("start: save_all_edinet_csv_doc_to_db")

        edinet_codes = self.get_edinet_codes()
        combined_df = pd.DataFrame()
        for index, edinet_code in enumerate(edinet_codes):
            try:
                if (index + 1) % batch_size == 0:
                    self.save_securities_report_to_db(combined_df)
                    combined_df = pd.DataFrame()
                res_df = self.get_securities_report_by_edinet_code(edinet_code, target_date_start, target_date_end)
                combined_df = pd.concat([combined_df, res_df], ignore_index=True)
                self.logger.info(f"edinet_code: {edinet_code}, count: {len(res_df)}")
            except Exception as e:
                self.logger.error(e)
                continue 
        if len(combined_df) > 0:
            self.save_securities_report_to_db(combined_df)

        self.logger.info("end: save_all_edinet_csv_doc_to_db")
    
    def get_edinet_codes(self) -> list[str]:
        
        self.logger.info("start: get_edinet_codes")

        database_url = f'sqlite:///{config.EDINET_DB}'
        manager = SqlUtils(database_url, EdinetcodeTable)
        columns = ["edinetCode"]

        select_conditions = {
            "submitterType": {"type": "string", "filter_type": "eq", "value": "内国法人・組合"},
            "listedSection": {"type": "string", "filter_type": "eq", "value": "上場"},
        }
        query_response = manager.get_with_compound_conditions(distinct=True, columns=columns, **select_conditions)


        edinet_codes = []
        for edinet_code_table in query_response:
            for column_name in columns:
                edinet_codes.append(getattr(edinet_code_table, column_name))
        self.logger.info("end: get_edinet_codes")
        return edinet_codes
    
    def save_edinet_codes(self):
        self.logger.info("start: save_edinet_codes")
        # ダウンロード先のURL
        url = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip"

        # ZIPファイルのダウンロード
        response = requests.get(url)
        zip_file = ZipFile(BytesIO(response.content))

        # CSVファイルの読み込み
        with zip_file.open('EdinetcodeDlInfo.csv') as file:
            df = pd.read_csv(file, encoding='cp932', skiprows=1)

        # 列名を英語に変更
        column_mappings = {
            "ＥＤＩＮＥＴコード": "edinetCode",
            "提出者種別": "submitterType",
            "上場区分": "listedSection",
            "連結の有無": "consolidation",
            "資本金": "capital",
            "決算日": "fiscalYearEnd",
            "提出者名": "submitterName",
            "提出者名（英字）": "submitterNameEn",
            "提出者名（ヨミ）": "submitterNameKana",
            "所在地": "address",
            "提出者業種": "industry",
            "証券コード": "securityCode",
            "提出者法人番号": "corporateNumber"
        }
        df.rename(columns=column_mappings, inplace=True)

        # SQLiteデータベースへの接続設定
        database_url = f"sqlite:///{config.EDINET_DB}"
        engine = create_engine(database_url)

        # データフレームをデータベースに保存
        df.to_sql('edinetcode_table', con=engine, if_exists='replace', index=False)
        self.logger.info("end: save_edinet_codes")

if __name__ == '__main__':
    edinet_utils = EdinetUtils()
    # res = edinet_utils.download_document(doc_id="S100SQL8")
    # res = edinet_utils.get_doc_id_list("E00015", "2000-01-01", "2024-03-20")
    # edinet_utils.get_doc_infos(days=3653)
    # get_doc_infos(days=3653)
    # df = pd.read_hdf('data/edinet.h5', key='document_list')
    # edinet_utils.save_tag_to_db("data/excel/ESE140114.xlsx")
    # edinet_utils.save_account_tag_to_db("data/excel/ESE140115.xlsx")
    # edinet_utils.get_securities_report_by_edinet_code("E00015", "2020-01-01", "2024-04-20")
    # edinet_utils.get_securities_report_by_edinet_code("E04430", "2020-01-01", "2024-04-20")
    # edinet_utils.save_all_edinet_csv_doc_to_db("2000-01-01", "2024-04-20")
    edinet_utils.save_all_edinet_csv_doc_to_db("2000-01-01", "2024-04-20", batch_size=1000)
    # res = edinet_utils.get_by_element_id("jppfs_cor:NetSales", "2023-03-31", "E00015")
    # edinet_utils.save_edinet_codes()
    # res = edinet_utils.get_edinet_codes()
    # print(res)
    # print(len(res))

    # res = edinet_utils.get_edinet_codes_df()
    # res.to_csv("data/edinet_codes.csv", index=False)

    
    