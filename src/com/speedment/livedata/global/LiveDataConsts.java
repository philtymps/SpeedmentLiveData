package com.speedment.livedata.global;

// constants
public class LiveDataConsts {

	public	static	final String	LIVEDATA_BEGINTABLE_IDENTIFIER = "--BEGINTABLE--";
	public	static	final String	LIVEDATA_ENDTABLE_IDENTIFIER = "--ENDTABLE--";
	public	static	final String	LIVEDATA_RESET_IDENTIFIER = "SPEEDMENT-RESET";
	public	static	final String	LIVEDATA_TABLE_PROPERTIES_FILE_NAME = "speedment-tables.properties";
	public	static	final String	LIVEDATA_CUSTOMER_OVERRIDES = "/customer_overrides.properties";
	
	public static	final int 		LIVEDATA_STATE_STARTING = 0;
	public static	final int 		LIVEDATA_STATE_PROCESSING_RECORDS = 2;
	public static	final int		LIVEDATA_STATE_FLUSH_KAFKA = 4;
	public static	final int		LIVEDATA_STATE_WRITING_FILE = 5;

	public static	final String 	LIVEDATA_DEFAULT_PROPERTIES_FILE_NAME = "speedment-livedata-client.properties";
	public static	final String	LIVEDATA_DEFAULT_LOG4J_FILE_NAME = "log4j.properties";
	
	public static	final String	LIVEDATA_DEFAULT_KAFKA_TOPIC = "speedment-topic";
	public static	final String	LIVEDATA_DEFAULT_KAFKA_SERVERS = "localhost:9092";
	public	static	int				LIVEDATA_DEFAULT_FETCHLIMIT = 500;
	
	public static	final String	LIVEDATA_ENCRYPTER_PREFIX = "encrypted:";
	
	public 	static	String			LIVEDATA_FIRSTRUN_PENDING = "FPENDING";
	public	static	String			LIVEDATA_PENDING = "PENDING";
	public	static	String			LIVEDATA_RUNNING = "RUNNING";
	public 	static	String			LIVEDATA_ABORTED = "ABORTED";
	public 	static	String			LIVEDATA_SUCCESS = "SUCCESS";
	public	static	String			LIVEDATA_FAILED  = "FAILED";
	public	static	String			LIVEDATA_RESET   = "RESET";
	
	public static	final String 	LIVEDATA_PRODUCER_FILE_NAME = "speedment-livedata-producer.properties";

	//list of tables to be parsed for COS upload
	public static	final String	OMS_TABLE_ORDER_HEADER = "YFS_ORDER_HEADER";
	public static	final String	OMS_TABLE_ORDER_LINE = "YFS_ORDER_LINE";
	
	//list of JSON general data attributes for COS upload
	public static	final String	SCIS_EVENT_CODE = "eventCode";	
	public static	final String	SCIS_EVENT_DETAILS = "eventDetails";
	public static	final String	SCIS_BUSINESS_OBJECT = "businessObject";
	public static	final String	SCIS_OBJECT_UPSERT_EVENT = "objectUpsertEvent";
	public static	final String	SCIS_TYPE_ORDER = "Order";
	public static	final String	SCIS_STERLING_SC_GLOBALID = "sterlingSupplyChain.globalId";
	
	//list of JSON order line data attributes for COS upload
	public static	final String	SCIS_ORDER_TYPE = "orderType";
	public static	final String	SCIS_TYPE = "type";
	public static	final String	SCIS_GLOBAL_IDENTIFIERS = "globalIdentifiers";
	public static	final String	SCIS_NAME = "name";
	public static	final String	SCIS_VALUE = "value";
	public static	final String	SCIS_VALUE_CURRENCY = "valueCurrency";
	public static	final String	SCIS_OUTBOUND = "OUTBOUTND";
	public static	final String	SCIS_INBOUND = "INBOUND";
	public static	final String	SCIS_TRANSFER = "TRANSFER";
	public static	final String	SCIS_ORDER_IDENTIFIER = "orderIdentifier";	
	public static	final String	SCIS_ORDER_LINES = "orderLines";
	public static	final String	SCIS_CREATED_DATE = "createdDate";
	public static	final String	SCIS_ORDER_LINE_NO = "orderLineNumber";	
	public static	final String	SCIS_PRODUCT = "product";
	public static	final String	SCIS_PRODUCT_VALUE = "productValue";
	public static	final String	SCIS_QUANTITY = "quantity";
	public static	final String	SCIS_QUANTITY_UNITS = "quantityUnits";
	public static	final String	SCIS_REQ_DELIVERY_DATE = "requestedDeliveryDate";
	public static	final String	SCIS_REQ_SHIP_DATE = "requestedShipDate";
	public static	final String	SCIS_SHIP_FROM_INSTR_LOCATION = "shipFromInstructionLocation";
	public static	final String	SCIS_SHIP_TO_LOCATION = "shipToLocation";
	
	//list of JSON order header data attributes for COS upload
	public static	final String	SCIS_BUYER = "buyer";
	public static	final String	SCIS_LINE_COUNT = "lineCount";
	public static	final String	SCIS_ORDER_VALUE_CURRENCY = "orderValueCurrency";
	public static	final String	SCIS_PLANNED_SHIP_DATE = "plannedShipDate";
	public static	final String	SCIS_PLANNED_DEL_DATE = "plannedDeliveryDate";
	public static	final String	SCIS_SOURCE_LINK = "sourceLink";
	public static	final String	SCIS_TOTAL_VALUE = "totalValue";
	public static	final String	SCIS_VENDOR = "vendor";
	public static	final String	SCIS_TENANT_ID = "tenantId";
	
	//list of oms order header table columns
	public static	final String	OMS_BILLTO_ID = "BILL_TO_ID";
	public static	final String	OMS_SHIP_ID = "SHIP_TO_ID";
	public static	final String	OMS_ORDER_DATE = "ORDER_DATE";
	public static	final String	OMS_ORDER_NO = "ORDER_NO";
	public static	final String	OMS_ORDER_HEADER_KEY = "ORDER_HEADER_KEY";
	public static	final String	OMS_CURRENCY = "CURRENCY";
	public static	final String	OMS_REQ_DELIVERY_DATE = "REQ_DELIVERY_DATE";
	public static	final String	OMS_REQ_SHIP_DATE = "REQ_SHIP_DATE";
	public static	final String	OMS_SELLEER_ORG = "SELLER_ORGANIZATION_CODE";
	public static	final String	OMS_TOTAL_AMT = "TOTAL_AMOUNT";
	
	//list of oms order line table columns
	public static	final String	OMS_ORDER_LINE_KEY = "ORDER_LINE_KEY";
	public static	final String	OMS_PRIME_LINE_NO = "PRIME_LINE_NO";
	public static	final String	OMS_ITEM_ID = "ITEM_ID";
	public static	final String	OMS_UNIT_PRICE = "UNIT_PRICE";
	public static	final String	OMS_ORDERED_QTY = "ORDERED_QTY";
	public static	final String	OMS_UOM = "UOM";
	public static	final String	OMS_SHIP_NODE = "SHIPNODE_KEY";
	public static	final String	OMS_LINE_TOTAL = "LINE_TOTAL";
	public static	final String	OMS_DOC_TYPE = "DOCUMENT_TYPE";
	public static	final String	OMS_SALES_ORDER = "0001";
	public static	final String	OMS_PURCHASE_ORDER = "0005";
	public static	final String	OMS_TRANSFER_ORDER = "0006";
	public static	final String	OMS_EARLIEST_SHIP_DATE = "EARLIEST_SHIP_DATE";
	public static	final String	OMS_EARLIEST_DELIVERY_DATE = "EARLIEST_DELIVERY_DATE";
	
	//list of COS related constants
	public static	final String	COS_ORDER_HEADER = "ORDER_HEADER";
	public static	final String	COS_ORDER_LINE = "ORDER_LINE";
	public static	final String	COS_ITEM = "ITEM";
	public static	final String	COS_CUSTOMER = "CUSTOMER";
	public static	final String	COS_CORG = "ORGANIZATION";	
}
