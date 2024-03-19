from sqlalchemy import create_engine, Column, Integer, String, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import config
from src.common.logger import SimpleLogger
from sqlalchemy import func

Base = declarative_base()

class SqlUtils:
    def __init__(self, database_url, model):
        self.engine = create_engine(database_url)
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)
        self.model = model
        self.logger = SimpleLogger(__class__.__name__)
        # モデルクラスのメタデータをデータベースに作成
        Base.metadata.create_all(self.engine)

    def add(self, **kwargs):
        self.logger.info(f"start: add, kwargs: {kwargs}")
        session = self.Session()
        instance = self.model(**kwargs)
        session.add(instance)
        session.commit()
        session.close()
        self.logger.info(f"end: add")
        return instance

    def get(self, **filters):
        self.logger.info(f"start: get, filters: {filters}")

        session = self.Session()
        results = session.query(self.model).filter_by(**filters).all()
        session.close()
        self.logger.info(f"end: get")
        return results

    def get_with_compound_conditions(self, **filters):
       
        query = self.Session.query(self.model)
        self.logger.info(f"start: get_with_compound_conditions, kwargs: {filters}")
        
        # その他の条件をconditionsリストに追加
        conditions = []
        for key, value in filters.items():
            print(value)
            attrib = getattr(self.model, key)
            if value["type"] == "date":
                attrib = func.date(attrib)
            if value["filter_type"] == "in":
                conditions.append(attrib.in_(value["values"]))
            elif value["filter_type"] == "between":
                conditions.append(attrib.between(value["start"], value["end"]))
            elif value["filter_type"] == "is_null":
                conditions.append(attrib.is_(None))
            elif value["filter_type"] == "is_not_null":
                conditions.append(attrib.isnot(None))
            elif value["filter_type"] == "like":
                conditions.append(attrib.like(value["value"]))
            elif value["filter_type"] == "not_like":
                conditions.append(attrib.notlike(value["value"]))
            elif value["filter_type"] == "eq":
                conditions.append(attrib == value["value"])
            else:
                raise ValueError(f"Invalid type: {value['type']}")
            
        # 全ての条件をand_で結合してfilterに適用
        query = query.filter(and_(*conditions))
        self.logger.info(f"end: get_with_compound_conditions")
        return query.all()
    
    def update(self, filters, **kwargs):
        self.logger.info(f"start: update, filters: {filters}, kwargs: {kwargs}")
        session = self.Session()
        objects = session.query(self.model).filter_by(**filters)
        for obj in objects:
            for key, value in kwargs.items():
                setattr(obj, key, value)
        self.logger.info(f"end: update")
        session.commit()
        session.close()

    def delete(self, **filters):
        self.logger.info(f"start: delete, filters: {filters}")
        session = self.Session()
        objects = session.query(self.model).filter_by(**filters)
        for obj in objects:
            session.delete(obj)
        session.commit()
        session.close()
        self.logger.info(f"end: delete")

class DocumentListTable(Base):
    __tablename__ = 'document_list_table'
    
    docID = Column(String, primary_key=True)
    edinetCode = Column(String)
    secCode = Column(String)
    JCN = Column(String)
    filerName = Column(String)
    fundCode = Column(String)
    ordinanceCode = Column(String)
    formCode = Column(String)
    docTypeCode = Column(String)
    periodStart = Column(String)
    periodEnd = Column(String)
    submitDateTime = Column(String)
    docDescription = Column(String)
    issuerEdinetCode = Column(String)
    subjectEdinetCode = Column(String)
    subsidiaryEdinetCode = Column(String)
    currentReportReason = Column(String)
    parentDocID = Column(String)
    opeDateTime = Column(String)
    withdrawalStatus = Column(String)
    docInfoEditStatus = Column(String)
    disclosureStatus = Column(String)
    xbrlFlag = Column(String)
    pdfFlag = Column(String)
    attachDocFlag = Column(String)
    englishDocFlag = Column(String)
    csvFlag = Column(String)
    legalStatus = Column(String)

# 使用例
if __name__ == "__main__":
    database_url = f'sqlite:///{config.EDINET_DB}'
    manager = SqlUtils(database_url, DocumentListTable)
    # document_list_tables = manager.get(docID="S100PFIV")
    document_list_tables = manager.get_with_compound_conditions(**{
        "submitDateTime": {"type": "date", "filter_type": "between", "start": "2022-11-01", "end": "2022-11-15"}
    })
    print([f"{document_list_table.docID},{document_list_table.edinetCode},{document_list_table.submitDateTime}" for document_list_table in document_list_tables])
