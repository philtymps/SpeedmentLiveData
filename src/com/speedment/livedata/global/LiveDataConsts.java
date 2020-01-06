package com.speedment.livedata.global;

// constants
public class LiveDataConsts {

	public	static	final String	LIVEDATA_BEGINTABLE_IDENTIFIER = "--BEGINTABLE--";
	public	static	final String	LIVEDATA_ENDTABLE_IDENTIFIER = "--ENDTABLE--";
	public	static	String			LIVEDATA_RESET_IDENTIFIER = "SPEEDMENT-RESET";
	public static	final String	LIVEDATA_TABLE_PROPERTIES_FILE_NAME = "speedment-tables.properties";
	
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
	
	
}
