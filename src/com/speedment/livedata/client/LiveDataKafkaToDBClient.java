package com.speedment.livedata.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

//import org.apache.log4j.BasicConfigurator;
//import org.apache.log4j.Level;
//import org.apache.log4j.xml.DOMConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import com.speedment.livedata.encrypter.LiveDataEncrypter;
import com.speedment.livedata.global.LiveDataConsts;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class LiveDataKafkaToDBClient {

	private static final String OPTION_VERBOSE = "-verbose";
	private static final String OPTION_PROPS = "-props:";
	private static final String OPTION_GENERATE_PWD = "-generatePassword";
	private static final String OPTION_SAVE = "-save:";
	private static final String OPTION_FLUSH_KAFKA = "-flush";
	
	private static final String OPTIN_USAGE1 = "-usage";
	private static final String OPTIN_USAGE2 = "-help";
	private static final String OPTIN_USAGE3 = "-?";
	
	// Class variables
	private Properties	m_Properties = null;
	private Properties	m_TableProperties = null;
	private boolean		m_bVerbose;
	private boolean		m_bSaveMessagesToFile;
	private	FileWriter	m_FileWriter = null;
	private KafkaConsumer<String, String> m_KafkaConsumer = null;
	private Connection	m_ConDstDB = null;
	
	//COS related variables
	static LiveDataKafkaToCOSRunner ktcRunner;
	
	// logger
	static Logger logger = Logger.getLogger(com.speedment.livedata.client.LiveDataKafkaToDBClient.class);
	//static YFCLogCategory logger = YFCLogCategory.instance(LiveDataKafkaToDBClient.class);

    public LiveDataKafkaToDBClient ()
    {
    	m_Properties = new Properties();
    	m_TableProperties = new Properties();
    	m_bVerbose = false;
    	m_KafkaConsumer = null;
    	m_ConDstDB = null;
    }
    
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		String 		props = LiveDataConsts.LIVEDATA_DEFAULT_PROPERTIES_FILE_NAME;
		String 		saveFile = null;
		int			state = LiveDataConsts.LIVEDATA_STATE_STARTING;
		
		final LiveDataKafkaToDBClient	ldkToDB = new LiveDataKafkaToDBClient();
		final LiveDataEncrypter			ldkEncrypter = new LiveDataEncrypter();

/*		LOG4J initialization - uses properties embedded in the speedment-livedata-client.properties file now
		String log4jConfigFile = System.getProperty("user.dir")
                + File.separator + LiveDataConsts.LIVEDATA_DEFAULT_LOG4J_FILE_NAME;
		BasicConfigurator.configure();
		PropertyConfigurator.configure(log4jConfigFile);
		logger.setLevel(Level.toLevel(System.getProperty("log4j.rootLogger")));
		DOMConfigurator.configure(log4jConfigFile);
*/
		//		String password = System.getProperty("encryptionPassword");
		ldkToDB.setVerboseFlag(false);
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith(OPTION_PROPS)) {
				try {
					props = args[i].substring(OPTION_PROPS.length());
				}
				catch (Exception e) {
					logger.info("Invalid properties file name.");
					printUsage();
					System.exit(0);
				}
			}
			else if (args[i].startsWith(OPTION_FLUSH_KAFKA))
			{
				state = LiveDataConsts.LIVEDATA_STATE_FLUSH_KAFKA;
			}
			else if (args[i].startsWith(OPTION_GENERATE_PWD))
			{
				ldkToDB.loadPropertiesFromFile(props);
		        String originalPassword = ldkToDB.getProperties().getProperty("speedment.consumer.database.DstDBPassword");
		        System.out.println("Original password: " + originalPassword);
		        String encryptedPassword = ldkEncrypter.encrypt(originalPassword);
		        System.out.println("Encrypted password: " + encryptedPassword);
		        String decryptedPassword = ldkEncrypter.decrypt(encryptedPassword);
		        System.out.println("Decrypted password: " + decryptedPassword);
		        System.exit(0);
			}
			else if (args[i].startsWith(OPTION_SAVE)) {
				try {
					saveFile = args[i].substring(OPTION_SAVE.length());
					state = LiveDataConsts.LIVEDATA_STATE_WRITING_FILE;
				}
				catch (Exception e) {
					System.out.println("Invalid save message file name.");
					printUsage();
					System.exit(0);
				}
			}
			else if (args[i].equals(OPTION_VERBOSE)) {
				ldkToDB.setVerboseFlag(true);
			}
			else if (args[i].equals(OPTIN_USAGE1) || args[i].equalsIgnoreCase(OPTIN_USAGE2) || args[i].equalsIgnoreCase(OPTIN_USAGE3)) {
				printUsage();
				System.exit(0);
			}
		}
		String sRecord = "";
		try {
			
			// load the properties file and decrypt database password property 
			ldkToDB.loadPropertiesFromFile(props);
			ldkToDB.getProperties().setProperty("speedment.consumer.database.DstDBPassword", ldkEncrypter.decryptIfEncrypted(ldkToDB.getProperties().getProperty("speedment.consumer.database.DstDBPassword")));
						
			// initialize log4j basic mode
			final Thread mainThread = Thread.currentThread();

			// Registering a shutdown hook so we can exit cleanly
			Runtime.getRuntime().addShutdownHook(new Thread() {
        	public void run() {
        		// Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
        		if (ldkToDB.getKafkaConsumer() != null)
        			ldkToDB.getKafkaConsumer().wakeup();
        		logger.info ("Terminating Speedment Live Data Consumer...");
        		try {
        			mainThread.join();
        			shutdown(ldkToDB);
            		logger.info ("Exiting...");
        		} catch (InterruptedException e) {
        			e.printStackTrace();
        		}
        		catch (Exception e) {
        			e.printStackTrace();
        		}
        	}
			});
			
            ldkToDB.loadTablePropertiesFromFile (LiveDataConsts.LIVEDATA_TABLE_PROPERTIES_FILE_NAME);
			ldkToDB.createKafkaConsumer();
            
    	    logger.info( "Enter Q or q to end..." );
    	    
			if (saveFile != null && saveFile.length() > 0) {
				ldkToDB.createSaveMessageFile (saveFile);
			}
			
			// if DB is enabled
			if (ldkToDB.IsDBEnabled())
			{
	        	if (ldkToDB.getVerboseFlag())
	        		logger.info ("Connecting to DB...");
	        	
	        	// get the database connection and set auto-commit off
	        	if (ldkToDB.getDSTConnection() == null)
	        	{
					logger.info ("Database Connection Failure: " + ldkToDB.getDBURL());
					throw (new Exception("Database Connection Failure:"));
	        	}
				ldkToDB.getDSTConnection().setAutoCommit(false);
			}
			
			if(ldkToDB.IsCOSEnabled()) {
				ktcRunner = new LiveDataKafkaToCOSRunner(ldkToDB);
			}
			
            // looping until ctrl-c, the shutdown hook will cleanup on exit
            String							sTableName = null;
            String							sTableColumns = null;
            List<String>					lstTableColumns = null;
            
            
            // start our polling loop
        	logger.info ("Beginning Polling of Kafka Topic: " + ldkToDB.getProperty ("speedment.consumer.kafka.topic"));
        	
        	int				iRecordsProcessed;
        	boolean			bQuitKeyPressed = false;
        	String			sKeyIdentifier = "";

        	while (true)
            {
            	ConsumerRecords<String, String> records = ldkToDB.getKafkaConsumer().poll(LiveDataConsts.LIVEDATA_DEFAULT_POLLING_MILLISECONDS);
            	iRecordsProcessed = 0;
            	
            	if (records.count() > 0)
            	{
            		if (ldkToDB.getVerboseFlag())
            			logger.info ("Polling returned " + records.count() + " records...");
            	}

            	ConsumerRecordLoop:
                for (ConsumerRecord<String, String> record : records)
                {
                	sRecord = ldkToDB.prepareRecordForSQL (record.value());
                	int				iExpectedCount = records.count();
                	List<String>	lstRecordValues = Arrays.asList (sRecord.split("\\s*,\\s*"));
                	boolean			bCanCommit = false;
                	
                	// get key identifier if any
                	sKeyIdentifier = lstRecordValues.get(0);
                	  	
                	if (ldkToDB.IsKeyIdentifier(sKeyIdentifier) && ldkToDB.getVerboseFlag())
                		logger.info ("Identifier: " + sKeyIdentifier + " State = " + state + " Task Id=" + lstRecordValues.toString());

                	if (state == LiveDataConsts.LIVEDATA_STATE_WRITING_FILE || state == LiveDataConsts.LIVEDATA_STATE_FLUSH_KAFKA)
                	{
                    	if(ldkToDB.IsSaveFileEnabled())
                    		if (!ldkToDB.writeSaveFileMessage("partition =  " + record.partition() + " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value()+ "\n", ldkToDB.IsKeyIdentifier(sKeyIdentifier) ? true : false))
                    			break ConsumerRecordLoop;
                    	bCanCommit = (iRecordsProcessed == iExpectedCount-1);
                	}
            		else if (sKeyIdentifier.equals (LiveDataConsts.LIVEDATA_BEGINTABLE_IDENTIFIER))
        			{
            			bCanCommit = false;
        				sTableName = lstRecordValues.get(2);
        				lstTableColumns = (List <String>)lstRecordValues.subList(3, lstRecordValues.size());
        				sTableColumns = lstTableColumns.toString();
        				sTableColumns = sTableColumns.substring(1, sTableColumns.length()-1);
        				ldkToDB.setTableProperty(sTableName, sTableColumns);
        			}
        			else if (sKeyIdentifier.equals (LiveDataConsts.LIVEDATA_ENDTABLE_IDENTIFIER))
        			{
        				bCanCommit = true;
        			}
                	else if (sKeyIdentifier.equals(LiveDataConsts.LIVEDATA_RESET_IDENTIFIER))
            		{
            			if (ldkToDB.IsDBEnabled())
            				bCanCommit = ldkToDB.processResetMessage (lstRecordValues);
            		}
                	else if (sKeyIdentifier.equals(LiveDataConsts.LIVEDATA_PUBLISH_IDENTIFIER))
            		{
                		if (ldkToDB.IsCOSEnabled())
                		{
            				if(ktcRunner.hasDataToPublish())
            				{
            	        		ktcRunner.publishCOSDataToCloud();
            					ktcRunner = new LiveDataKafkaToCOSRunner(ldkToDB);
            	        	}
                		}
                		bCanCommit = true;
            		}
                	else
                	{
            			String	sRecordKey = record.key();
            			sTableName = sRecordKey.substring(0, sRecordKey.indexOf('-'));
            			sTableColumns = ldkToDB.getTableProperty(sTableName);
            			lstTableColumns = Arrays.asList (sTableColumns.split("\\s*,\\s*"));
            			if (!ldkToDB.processDstDataRecord (sTableName, sTableColumns, lstTableColumns, lstRecordValues))
            				break ConsumerRecordLoop;
                	}
                	iRecordsProcessed++;
                	if (state == LiveDataConsts.LIVEDATA_STATE_STARTING)
                		state = LiveDataConsts.LIVEDATA_STATE_PROCESSING_RECORDS;
                	
                    // commit to DB and commit the topic position to mark these records as consumed by this consumer
                    if (bCanCommit)
                    {
                    	if (ldkToDB.getVerboseFlag() && (iRecordsProcessed % 100) == 0)
                    		logger.info ("Processed " + iRecordsProcessed + " Records Successfullly...\r");
                    	
                    	// perform the db commit first
                    	if (ldkToDB.IsDBEnabled() && 
                    	  !(state == LiveDataConsts.LIVEDATA_STATE_WRITING_FILE || state == LiveDataConsts.LIVEDATA_STATE_FLUSH_KAFKA))
                    		ldkToDB.getDSTConnection().commit();
            			// perform the kafka commit next - at this point database and kafka topic are in synch
            			if (ldkToDB.IsKafkaEnabled())
                    		ldkToDB.commitKafkaConsumer ();
            			bCanCommit = false;            			
                    }
                }
            	if (iRecordsProcessed > 0)
            	{
            		if (ldkToDB.getVerboseFlag()) {
                		logger.info ("Processed " + iRecordsProcessed + " Records Successfullly...");
                		logger.info ("Polling......");            			
            		}
            	}
            	
    			// it's safe at this point to look for a quit signal
                if (bQuitKeyPressed || ldkToDB.IsQuitKeyPressed())
                {
                	if (!bQuitKeyPressed)
                		logger.info ("Shutdown Gracefully...Please Wait...");
                	bQuitKeyPressed = true;
                	if (sKeyIdentifier.equalsIgnoreCase (LiveDataConsts.LIVEDATA_ENDTABLE_IDENTIFIER)
                	||  state == LiveDataConsts.LIVEDATA_STATE_STARTING
                	||  state == LiveDataConsts.LIVEDATA_STATE_WRITING_FILE
                	||  state == LiveDataConsts.LIVEDATA_STATE_FLUSH_KAFKA)
            			break;                	
            	}
            }
        	
        	
        } catch (WakeupException e) {
        	logger.info("Stopped Abnormally..Cleaning up...");
        } catch (NullPointerException e) {
        	if (ldkToDB.getVerboseFlag())
        		e.printStackTrace();
        } catch (Exception e) {
        	logger.info (e.getClass() + " " + e.getMessage());
        	if (ldkToDB.getVerboseFlag())
        		e.printStackTrace();
        }
    }
    
    private static void shutdown(LiveDataKafkaToDBClient ldkToDB) throws Exception
    {
		// save the table properties file
    	ldkToDB.saveTablePropertiesToFile (LiveDataConsts.LIVEDATA_TABLE_PROPERTIES_FILE_NAME);

		if (ldkToDB.IsSaveFileEnabled())
			ldkToDB.closeSaveMessageFile();

		if (ldkToDB.IsKafkaEnabled() && ldkToDB.getKafkaConsumer() != null)
		{
			ldkToDB.getKafkaConsumer().close();
		}
		if (ldkToDB.IsDBEnabled() && ldkToDB.getDSTConnection() != null)
		{
				ldkToDB.getDSTConnection().close();
		}
    }

    protected	boolean	IsKeyIdentifier (String sKeyIdentifier)
    {
    	return (sKeyIdentifier.equals(LiveDataConsts.LIVEDATA_BEGINTABLE_IDENTIFIER) || sKeyIdentifier.equals(LiveDataConsts.LIVEDATA_ENDTABLE_IDENTIFIER) || sKeyIdentifier.equals(LiveDataConsts.LIVEDATA_RESET_IDENTIFIER) || sKeyIdentifier.equals(LiveDataConsts.LIVEDATA_PUBLISH_IDENTIFIER));
    }
    protected	boolean	IsKafkaEnabled ()
    {
    	return (Boolean.valueOf((String)getProperty("speedment.consumer.kafka.enabled")));
    }
    
    protected boolean	IsSaveFileEnabled ()
    {
    	return m_bSaveMessagesToFile;
    }

    protected	void	createKafkaConsumer()
    {
		m_KafkaConsumer = new KafkaConsumer<>(initializeKafkaProperties(), new StringDeserializer(), new StringDeserializer());
        m_KafkaConsumer.subscribe(Collections.singletonList((String)getProperty("speedment.consumer.kafka.topic")));
    }
    
    protected	KafkaConsumer<String, String> getKafkaConsumer()
    {
    	return m_KafkaConsumer;
    }
    
    protected	void commitKafkaConsumer ()
    {
        for (TopicPartition tp : m_KafkaConsumer.assignment())
        	m_KafkaConsumer.position(tp);
        m_KafkaConsumer.commitSync();
    }
    
    protected	Properties getTableProperties()
    {
    	return m_TableProperties;
    }
    protected	void	setTableProperty (String sTableName, String sTableColumns)
    {
    	m_TableProperties.setProperty(sTableName, sTableColumns);
    }
    protected	String	getTableProperty (String sTableName)
    {
    	return m_TableProperties.getProperty(sTableName);
    }
    
    protected	void	loadTablePropertiesFromFile (String sFileName) throws Exception
    {
    	try {
    		FileReader	fileReader = new FileReader (sFileName);
    		m_TableProperties.load(fileReader);
    	} catch (FileNotFoundException e) {
    		m_TableProperties = new Properties ();
    	} catch (IOException e) {
			if (getVerboseFlag())
				logger.info ("Exception in loadTablePropertiesFile: " + e.getClass() + " " + e.getMessage());
				throw new Exception (e.getMessage());
		}
    }

    protected	void	saveTablePropertiesToFile (String sFileName)
    {
		try {
			FileWriter fileWriter = new FileWriter(sFileName);
			m_TableProperties.store(fileWriter, "LiveDataClient");
			
			if (getVerboseFlag())
				logger.info ("Writing to Properties File..." + sFileName);
		}
		catch (IOException e) {
			if (getVerboseFlag())
				logger.info ("Exception in saveTablePropertiesToFile: " + e.getClass() + " " + e.getMessage());
		}
    }

    protected	void	createSaveMessageFile (String sFileName) throws Exception
    {
		try {
			m_FileWriter = new FileWriter(sFileName);
			m_bSaveMessagesToFile = true;
			
			if (getVerboseFlag())
				logger.info ("Writing to File..." + sFileName);
		}
		catch (IOException e) {
			if (getVerboseFlag())
				logger.info ("Exception in createSaveMessageFile: " + e.getClass() + " " + e.getMessage());
			throw new Exception (e.getMessage());
		}
    }
    
    protected	boolean writeSaveFileMessage (String sMessageToWrite, boolean bFlush) throws Exception
    {
    	boolean bCanCommit = false;
    	try {
    		m_FileWriter.write(sMessageToWrite);
    		if (bFlush)
    			m_FileWriter.flush();
    		bCanCommit = true;
    	}
    	catch (IOException e) {
    		logger.info("Failed to write to the save message file...");
    		if (getVerboseFlag()) {
    				logger.info ("Exception in writeSaveMessageFile: " + e.getClass() + " " + e.getMessage());
    				throw new Exception (e.getMessage());
    		}
    	}
    	return bCanCommit;
    }
    
    protected	void closeSaveMessageFile () throws Exception
    {
    	try {
    		m_FileWriter.close();
    	}  catch (IOException e) {
    		logger.info("Failed to close the save message file...");
    		if (getVerboseFlag()) {
    				logger.info ("Exception in WriteSaveMessageFile: " + e.getClass() + " " + e.getMessage());
    				throw new Exception (e.getMessage());
    		}
    	}
    }
    
    protected	String	prepareRecordForSQL (String sRecord)
    {
    	// replace single quote characters with two single quote character's as SQL escaping is required
    	sRecord = sRecord.replace("'", "''");
    	
    	// replace all double quotes (field being/end markers) with single quote
    	return sRecord.replace("\"", "'");
    }
    
    /* ONLY WORKS ON FULLY FUNCTIONAL JDBC DRIVERS
	protected void	processDstDataRecord (String sTableName, String sTableColumns, List<String> lstTableColumns, List<String> lstRecordValues) throws Exception
	{
		try {
			// now create a connection to the destination database
			String				sSQL = "SELECT " + sTableColumns + " FROM " + sTableName + " WHERE " + lstTableColumns.get(0) + "= '" + lstRecordValues.get(0) + "' FOR UPDATE";
			PreparedStatement	ps = conDstDB.prepareStatement(sSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			ResultSet			rsResults = ps.executeQuery();
			
			// perform an update
			if (rsResults.next())
			{
				int		iColumn = 1;
				boolean	bUpdateRow  = false;
				for (String sColumn : lstTableColumns)
				{
					String sCurrentValue = rsResults.getString (iColumn);
					String sNewValue = lstRecordValues.get (iColumn-1);
					if (sCurrentValue.equals(sNewValue))
					{
						rsResults.updateString(iColumn, sNewValue);
						bUpdateRow = true;
					}
					if (bUpdateRow)
						rsResults.updateRow();
				}
			}
			// perform an insert
			else
			{
				// perform an insert
				rsResults.moveToInsertRow();
				int		iColumn = 1;
				for (String sColumn : lstTableColumns)
				{
					String sNewValue = lstRecordValues.get (iColumn-1);
					rsResults.updateString(iColumn, sNewValue);
				}
				rsResults.insertRow();
				rsResults.moveToCurrentRow();
			}
			ps.close();
			rsResults.close();
		} catch (SQLException e) {
			  throw new Exception (e.getMessage());
		}
		return;
	 }
     */
    
    protected boolean	processDstDataRecord (String sTableName, String sTableColumns, List<String> lstTableColumns, List<String> lstTableValues) throws Exception
    {
    	if (IsDBEnabled())
    		return processDstDataRecordV1 (sTableName, sTableColumns, lstTableColumns, lstTableValues);
    	
    	if (IsCOSEnabled())
    		return ktcRunner.processCOSDataRecord(sTableName, sTableColumns, lstTableColumns, lstTableValues);
    	else
    		return true;
    }
    
	protected boolean	processDstDataRecordV1 (String sTableName, String sTableColumns, List<String> lstTableColumns, List<String> lstTableValues) throws Exception
	{
		boolean	bCanCommit = false;
		StringBuilder	sSQL = new StringBuilder();

		try {
			boolean			bRecordExists;
			int				iTableValuesInitialOffset = 0;
			List<String>	lstURLEncodedColumns = Arrays.asList(((String)getProperty("speedment.urlencoded.columns")).split ("\\s*,\\s*"));

			if (bRecordExists = checkRecordExists(sTableName, lstTableColumns.get(0), lstTableValues.get(iTableValuesInitialOffset)))
			{
				// perform an update
				sSQL.append("UPDATE " + sTableName);
			}
			else
				sSQL.append("INSERT INTO " + sTableName);

			int iTableValuesOffset = iTableValuesInitialOffset;
			if (bRecordExists)
			{
				sSQL.append(" SET ");
				for (String sTableColumn : lstTableColumns)
				{
					String	sTableValue;
					if (iTableValuesOffset > iTableValuesInitialOffset)
					{
						sTableValue = lstTableValues.get(iTableValuesOffset);
						if (lstURLEncodedColumns.contains(sTableColumn))
						{
							// get the decoded value and then escape ' with ''
							sTableValue = URLDecoder.decode(sTableValue.substring(1,sTableValue.length()-1), "UTF-8");
							sTableValue = "'" + sTableValue.replace("'", "''") + "'";
						}

						sSQL.append(sTableColumn + "=" + sTableValue);
						if (iTableValuesOffset - iTableValuesInitialOffset + 1 < lstTableColumns.size())
							sSQL.append(',');
					}
					iTableValuesOffset += 1;
				}
			}
			else
			{
				sSQL.append(" VALUES(");
				for (String sTableValue : lstTableValues)
				{
					if (iTableValuesOffset >= iTableValuesInitialOffset)
					{
						if (iTableValuesOffset > iTableValuesInitialOffset)
							sSQL.append(',');
						if (lstURLEncodedColumns.contains(lstTableColumns.get(iTableValuesOffset-iTableValuesInitialOffset)))
						{
							// get the decoded value and then escape ' with ''
							sTableValue = URLDecoder.decode(sTableValue.substring(1,sTableValue.length()-1), "UTF-8");
							sTableValue = "'" + sTableValue.replace("'", "''") + "'";
						}
						sSQL.append(sTableValue);
					}
					iTableValuesOffset++;
				}
				sSQL.append(')');
			}
			if (bRecordExists)
				sSQL.append(" WHERE " + lstTableColumns.get(0) + "=" +  lstTableValues.get(iTableValuesInitialOffset));
			
			if (Boolean.valueOf(System.getProperty("SQLDebug")))
				logger.info (sSQL);
			// Execute the INSERT or UPDATE
			Statement	stmt = getDSTConnection().createStatement();
			
			stmt.execute(sSQL.toString());
			stmt.close();
			bCanCommit = true;
			
		} catch (SQLException e) {
			logger.info ("Exception in processDstDataRecordV1:" + e.getClass() + " " + e.getMessage());
			  throw new Exception (e.getMessage());
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.info ("Exception in processDstDataRecordV1:" + e.getClass() + " " + e.getMessage());
			logger.info ("URL Encoding May Be Required for the following Record");
			logger.info (sTableName + "Table Column Names: " + lstTableColumns.toString());
			logger.info (sTableName + "Table Column Values: " + lstTableValues.toString());
			logger.info("SQL Statement: " + sSQL);
		} catch (Exception e) {
			logger.info("Exception in processDstDataRecordV1:" + e.getClass() + " " + e.getMessage());
		}
		return (bCanCommit);
	}
	
/*
	protected boolean	processDstDataRecordV2 (Connection conDstDB, String sTableName, String sTableColumns, List<String> lstTableColumns, List<String> lstTableValues) throws Exception
	{
		int	iInsertedOrUpdatedCnt = 0;
		try {
			StringBuilder	sSQL = new StringBuilder();
			boolean			bRecordExists;
			if (bRecordExists = checkRecordExists(conDstDB, sTableName, lstTableColumns.get(0), lstTableValues.get(0)))
			{
				// perform an update
				sSQL.append("UPDATE " + sTableName);
			}
			else
				sSQL.append("INSERT INTO " + sTableName);

			int iTableColumn = 0;
			if (bRecordExists)
			{
				sSQL.append(" SET ");
				for (String sTableColumn : lstTableColumns)
				{
					if (iTableColumn > 1)
					{
						sSQL.append(sTableColumn + "=?");
						if (iTableColumn < lstTableColumns.size()-1)
							sSQL.append(',');
					}
					iTableColumn++;
				}
			}
			else
			{
				sSQL.append(" VALUES(");
				for (String sTableValue : lstTableValues)
				{
					if (iTableColumn++ > 0)
						sSQL.append(',');
					sSQL.append('?');
				}
				sSQL.append(')');
			}
			if (bRecordExists)
				sSQL.append(" WHERE " + lstTableColumns.get(0) + "=" +  lstTableValues.get(0));
			if (IsDebugging())
			{
				if (IsDebugging ())
				{
					logger.info ("INSERT/UPDATE TABLE using SQL:");
					logger.info (sSQL);
				}
			}
			// Execute the INSERT or UPDATE
			PreparedStatement	stmt = conDstDB.prepareStatement(sSQL.toString());
			int	iColumn = 1;
			List	lstURLEncodedColumns = Arrays.asList(((String)getProperty("speedment.urlencoded.columns")).split ("\\s*,\\s*"));
			for (String sTableValue : lstTableValues)
			{
				if (lstURLEncodedColumns.contains(lstTableColumns.get(iColumn-1)))
					sTableValue = URLDecoder.decode(sTableValue, "UTF-8");
				stmt.setString(iColumn++, sTableValue);
			}

			stmt.execute(sSQL.toString());
			iInsertedOrUpdatedCnt = stmt.getUpdateCount();
			stmt.close();
			
		} catch (SQLException e) {
			logger.info ("Exception in processDstDataRecord:" + e.getClass() + " " + e.getMessage());
			  throw new Exception (e.getMessage());
		}
		return (iInsertedOrUpdatedCnt > 0);
	}
*/

	private	boolean	checkRecordExists (String sTableName, String sPrimaryKeyName, String sPrimaryKeyValue) throws Exception
	{
		boolean bExists = false;
		try {
			// now create a connection to the destination database
			String				sSQL = "SELECT * FROM " + sTableName + " WHERE " + sPrimaryKeyName + " = " + sPrimaryKeyValue;
			
			if (Boolean.valueOf(System.getProperty("SQLDebug")))
				logger.info (sSQL);
			PreparedStatement	ps = getDSTConnection().prepareStatement(sSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet			rsResults = ps.executeQuery();
			
			// perform an update
			bExists = rsResults.next();
			ps.close();
			rsResults.close();
		} catch (SQLException e) {
			logger.info ("Exception in checkRecordExists:" + e.getClass() + " " + e.getMessage());
			throw new Exception (e.getMessage());
		}
		return bExists;
	}
	
    public	boolean processResetMessage (List<String> lstTableToReset) throws Exception
    {
      boolean bCanCommit = false;
      
      //  skip if db not enabled
      if (!IsDBEnabled())
    	  return true;      

      try {
		if (lstTableToReset.get(0).equals(LiveDataConsts.LIVEDATA_RESET_IDENTIFIER))
		{
			String			sDBAction = (String)System.getProperty ("DBAction");
			String			sTableName = lstTableToReset.get(2);
			
			if (sDBAction == null)
				sDBAction = lstTableToReset.get(1);

			sDBAction = sDBAction.toUpperCase();
			while (!sDBAction.equalsIgnoreCase("NONE"))
			{
				StringBuilder 	sSQL = new StringBuilder();

				if (getVerboseFlag())
					logger.info ("Processing " + lstTableToReset.get(0) + "," + sDBAction + "," + lstTableToReset.get(2) + "," + lstTableToReset.get(3));


				// DBAction can be one of DELETE, DROP, TRUNCATE or CREATE
				if (sDBAction.equalsIgnoreCase("DELETE"))
				{
					sSQL.append("DELETE FROM " + sTableName);
				}
				else if (sDBAction.equalsIgnoreCase("DROP") || sDBAction.equalsIgnoreCase("DROPANDCREATE"))
				{
					sSQL.append("DROP TABLE IF EXISTS " + sTableName);
				}
				else if (sDBAction.equalsIgnoreCase("CREATE"))
				{
					sSQL.append("CREATE TABLE IF NOT EXISTS \"" + sTableName + "\"");
					List<String> 		lstColumnNamesAndLengths = lstTableToReset.subList(3, lstTableToReset.size());
					sSQL.append(" (");
					Iterator<String>	iColumnsAndLengths = lstColumnNamesAndLengths.iterator();
					while (iColumnsAndLengths.hasNext())
					{
						sSQL.append("\"" + iColumnsAndLengths.next() + "\" TEXT");
						String	sColLength = iColumnsAndLengths.next();
						sSQL.append ("(" + sColLength + ")");
						sSQL.append(',');
					}
					sSQL.append(" PRIMARY KEY(\"" + lstColumnNamesAndLengths.get(0) + "\"))");
				}
				Statement stmt = getDSTConnection().createStatement();
				try {
					if (System.getProperty("SQLDebug") != null)
						logger.info (sSQL);
					stmt = getDSTConnection().createStatement();
					stmt.execute(sSQL.toString());
					bCanCommit = true;
				} catch (Exception e) {
				  bCanCommit = false;
		  	      if (getVerboseFlag())
		  	  		  logger.info ("Exception Executing SQL in processResetMessage: " + e.getClass() + e.getMessage());
				} finally {
					stmt.close();
				}
				// if we're dropping and creating do the create next
				if (sDBAction.equalsIgnoreCase("DROPANDCREATE") && bCanCommit)
				{
					sDBAction = "CREATE";
					getDSTConnection().commit ();
				}
				else
					sDBAction = "NONE";
			}
		  }
		} catch (Exception e) {
	  	      if (getVerboseFlag())
	  	  		  logger.info ("Exception in processResetMessage " + e.getClass() +" " + e.getMessage());
		}
  	    return bCanCommit;
    }
    
	@SuppressWarnings("unused")
	private boolean IsDebugging() {
		return getVerboseFlag();
	}

	public static void printUsage() {
		System.out.println("Usage:");
		System.out.println("   SpeedmentLiveDataClient [-props:<properties_file>] [-save:<save_message_file>]");
		System.out.println("             [-generatePassword] [-verbose] [-help|-usage|-?]");
		System.out.println("\n\t -props:<properties_file>");
		System.out.println("\t\t Allows to specify the properties file name that contains client ");
		System.out.println("\t\t information about the Kafka consumer and other important properties.");
		System.out.println("\t\t If <properties_file> is not specified, this program");
		System.out.println("\t\t attempts to find and use '" + LiveDataConsts.LIVEDATA_DEFAULT_PROPERTIES_FILE_NAME + "' on the root");
		System.out.println("\t\t of classpath.");
		System.out.println("\n\t -generatePassword");
		System.out.println("\t\t Encrypts the plain text property speedment.consumer.database.DstDBPassword");
		System.out.println("\t\t and prints out its encrypted value to replace in the properties files");
		System.out.println("\n\t -save:<save_message_file>");
		System.out.println("\t\t Indicates that incoming message should be just saved to a file");
		System.out.println("\t\t without being processed by the consumer. <save_message_file>");
		System.out.println("\t\t allows you to specify the name of the file where all the incoming");
		System.out.println("\t\t messages are to be saved. ");
		System.out.println("\n\t -flush");
		System.out.println("\t\t Flush Kafka Topic of all outstanding messages");
		System.out.println("\n\t -verbose");
		System.out.println("\t\t Runs in verbose mode (prints extra info on execution).");
		System.out.println("\n\t -help|-usage|-?");
		System.out.println("\t\t Outputs this usage information.");
		System.out.println("\nExample:");
		System.out.println("\t SpeedmentLiveDataClient -props:speedment-livedata-client.properties");
		System.out.println("\t SpeedmentLiveDataClient -props:speedment-livedata-client.properties -save:message.txt -verbose");
		System.out.println("\t SpeedmentLiveDataClient -props:speedment-livedata-client.properties -generatePassword");
		System.out.println("\nVMArgs Available:");
		System.out.println("\t\t -DBAction=DELETE|DROP|CREATE|DROPANDCREATE -DSQLDebug=true|false");
		System.out.println("\t\t Where:");
		System.out.println("\t\t DBAction overrides value coming from Agent Server.");
		System.out.println("\t\t SQLDebug will display SQL being executed on the Destination database while running.");
		
		System.exit(0);

	}

	protected	void	loadLog4JProperties () throws IOException, Exception
	{
		logger = Logger.getLogger(getClass());
		String	sLog4jPrefix = "log4j.";
		Properties propLog4j = new Properties();
		
		Enumeration<?>	enumPropNames = getProperties().propertyNames();
		while (enumPropNames.hasMoreElements())
		{
			String	sPropName = (String)enumPropNames.nextElement();
			if (sPropName.startsWith(sLog4jPrefix))
				propLog4j.setProperty(sPropName, (String)getProperties().get(sPropName));
		}
		if (getVerboseFlag())
		{
			logger.info ("LOG4J PROPERTIES USED TO CONNECT TO LOG TOPIC: ");
			debugProperties(propLog4j);
		}

//		Properties props = new Properties();
//		props.load(getClass().getResourceAsStream(sPropertyFileName));
		PropertyConfigurator.configure(propLog4j);
	}
	
	protected	void	loadPropertiesFromFile (String sPropertyFileName) throws IOException, Exception
	{
		m_Properties.load(getClass().getResourceAsStream(sPropertyFileName));
		Enumeration<?> enumPropertyNames = m_Properties.propertyNames();
		while (enumPropertyNames.hasMoreElements())
		{
			String	sPropertyName = (String) enumPropertyNames.nextElement();
			if (sPropertyName.startsWith(LiveDataConsts.LIVEDATA_ENCRYPTER_PREFIX))
			{
				String	sPropertyValue = m_Properties.getProperty(sPropertyName);
				LiveDataEncrypter	ldeEncrypter = new LiveDataEncrypter ();
				m_Properties.setProperty(sPropertyName, ldeEncrypter.decrypt(sPropertyValue.substring(LiveDataConsts.LIVEDATA_ENCRYPTER_PREFIX.length())));
			}
			
		}
		
		if (getVerboseFlag())
		{
			logger.info("SPEEDMENT CONSUMER PROPERTIES LOADED FROM: " + sPropertyFileName);
			debugProperties (m_Properties);
		}
		loadLog4JProperties ();
	}
	
	public boolean IsQuitKeyPressed() throws Exception
	{
	    char answer = 'x';
	    if (System.in.available() > 0)
	    {
		    InputStreamReader inputStreamReader = new InputStreamReader(System.in);
	    	answer = (char) inputStreamReader.read();
	    	logger.info("Initiating Shutdown...");
		    return(( answer == 'q' ) || ( answer == 'Q' ));
	    }
	    return false;
	}

	protected Properties	initializeKafkaProperties ()
	{
		Properties	propKafka = new Properties();
		String		sKafkaPropPrefix = "speedment.consumer.kafka.";
		Enumeration<?>	enumPropNames = getProperties().propertyNames();
		while (enumPropNames.hasMoreElements())
		{
			String	sPropName = (String)enumPropNames.nextElement();

			// enabled and topic are not a real Kafka properties so don't add it
			if (sPropName.equals("speedment.consumer.kafka.enabled")
			||  sPropName.equals("speedment.consumer.kafka.topic"))
				continue;

			if (sPropName.startsWith(sKafkaPropPrefix))
				propKafka.setProperty(sPropName.substring(sKafkaPropPrefix.length()), (String)getProperties().get(sPropName));
		}
		if (getVerboseFlag())
		{
			logger.info ("KAFKA PROPERTIES USED TO CONNECT TO KAFKA TOPIC: ");
			debugProperties(propKafka);
		}
		
		// since we're using transactional delivery we must set these properties
/*
		kafkaProps.put("bootstrap.servers", "diab191.tympsnet.com:9092");
		kafkaProps.put("transactional.id",  "SPEEDMENT-RESET");
		kafkaProps.put("group.id", "SPEEDMENT");
		kafkaProps.put("auto.offset.reset", "earliest");
		kafkaProps.put("enable.auto.commit", "false");
		kafkaProps.put("auto.commit.interval.ms", "5000");
		
		//kafkaProps.put("isolation.level", "read_committed");
		// these properties below are if here if we decided against using transactional delivery
		kafkaProps.put("acks", "all");
		kafkaProps.put("retries", 0);
		kafkaProps.put("batch.size", 16384);
		kafkaProps.put("linger.ms", 1);
		kafkaProps.put("buffer.memory", 33554432);
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
*/

		return propKafka;
	}

	 public	void debugProperties (Properties pProps)
	 {
		Enumeration<?>	enumPropNames = pProps.propertyNames();
		LiveDataEncrypter	ldkEncrypter = new LiveDataEncrypter ();
		while (enumPropNames.hasMoreElements())
		{
			String	sPropName = (String)enumPropNames.nextElement();
			String	sPropValue = pProps.getProperty(sPropName);
			if (sPropValue.startsWith(LiveDataConsts.LIVEDATA_ENCRYPTER_PREFIX))
			{
				 sPropValue = ldkEncrypter.decryptIfEncrypted(sPropValue);
				 if (sPropValue.length() > 4)
				 {
					 StringBuilder sMaskedProperty = new StringBuilder();
					 for (int iMaskedTotalLength = 0; iMaskedTotalLength < sPropValue.length()-4; iMaskedTotalLength++)
					 	sMaskedProperty.append('*');
					 sMaskedProperty.append(sPropValue.substring(sPropValue.length() - 4));
					 sPropValue = sMaskedProperty.toString();
				 }
			}
			logger.info (sPropName + "=" + sPropValue);
		}
	 }
	 
	  protected boolean IsDBEnabled ()
	  {
		  return ((Boolean)Boolean.valueOf((String)getProperty("speedment.consumer.database.enabled")));
	  }


	 public void setProperty(String sProp, String sValue)
	 {
		m_Properties.put(sProp, sValue);
	 }

	 protected Object getProperty(String sProp)
	 {
		return m_Properties.get(sProp);
	 }

	 protected	Properties	getProperties ()
	 {
		return m_Properties;
	 }

	 /*
	 *
	 * Connection Method for the Target System
	 *
	 */ 
	 protected String getDstDBType()
	 {
		if (getProperty("speedment.consumer.database.DstDBType") != null){
			return (String) getProperty("speedment.consumer.database.DstDBType");
		}
		return "DB2";
	 }

	 protected String getDstDBServer()
	 {
		if (getProperty("speedment.consumer.database.DstDBServer") != null){
			return (String) getProperty("speedment.consumer.database.DstDBServer");
		}
		return "oms.omfulfillment.com";
	 }

	 protected String getDstDBPort()
	 {
		if (getProperty("speedment.consumer.database.DstDBPort") != null){
			return (String) getProperty("speedment.consumer.database.DstDBPort");
		}
		return "50000";
	 }

	 protected String getDstDBDatabase()
	 {
		if (getProperty("speedment.consumer.database.DSTDatabase") != null){
			return (String) getProperty("speedment.consumer.database.DSTDatabase");
		}
		return "OMDB";
	 }

	protected String getDstDBUsername()
	{
		if (getProperty("speedment.consumer.database.DstDBUsername") != null){
			return (String) getProperty("speedment.consumer.database.DstDBUsername");
		}
		return "demouser";
	}

	protected String getDstDBPassword()
	{
		if (getProperty("speedment.consumer.database.DstDBPassword") != null){
			return (String) getProperty("speedment.consumer.database.DstDBPassword");
		}
		return "meeting265bridge";
	}

	protected String getDstDBSchema()
	{
		if (getProperty("speedment.consumer.database.DstDBSchema") != null){
			return (String) getProperty("speedment.consumer.database.DstDBSchema");
		}
		return "OMDB";
	}

	protected Connection getDSTConnection() throws SQLException, ClassNotFoundException
	{
		if (m_ConDstDB == null)
			m_ConDstDB = getDBConnection(getDstDBType(), getDstDBServer(), getDstDBPort(), getDstDBDatabase(), getDstDBUsername(), getDstDBPassword());
		return m_ConDstDB;
	}

	public String getDBURL()
	{
		return getDBURL(getDstDBType(), getDstDBServer(), getDstDBPort(), getDstDBDatabase());
	}

	private String getDBURL(String _dbType, String _sServer, String _sPort, String _sDatabase)
	{
		String jdbc = "";
		if (_dbType.equals("DB2")){
			jdbc = "jdbc:db2://"+_sServer+":"+_sPort+"/"+_sDatabase;
		} else if (_dbType.equals("Oracle")){
			jdbc = "jdbc:oracle:thin:@"+_sServer+":"+_sPort+":"+_sDatabase;
		} else if (_dbType.equals("sqlite")){
			jdbc = "jdbc:sqlite:" + _sDatabase;
		}
		return jdbc;
	}

	private String getJDBCDriver(String _dbType)
	{
		if (_dbType.equals("DB2")){
			return "com.ibm.db2.jcc.DB2Driver";
		} else if (_dbType.equals("Oracle")){
			return "oracle.jdbc.driver.OracleDriver";
		} else if (_dbType.equals("sqlite")) {
			return "org.sqlite.JDBC";
		}
		return null;
	}

	private Connection getDBConnection(String _dbType, String _sServer, String _sPort, String _sDatabase, String _sUserName, String _sPassword) throws ClassNotFoundException, SQLException
	{
		Class.forName(getJDBCDriver(_dbType));
		Connection jdbcConnection = (Connection)DriverManager.getConnection(getDBURL(_dbType, _sServer, _sPort, _sDatabase), _sUserName, _sPassword);
		return jdbcConnection;
	}
	
	public void setProperties(Properties m_Props)
	{
		this.m_Properties = m_Props;
	}

	public boolean getVerboseFlag()
	{
		return m_bVerbose;
	}

	public void setVerboseFlag(boolean m_bVerbose)
	{
		this.m_bVerbose = m_bVerbose;
	}

	private boolean IsCOSEnabled() {
		return ((Boolean)Boolean.valueOf((String)getProperty("speedment.consumer.cloudobjectstorage.enabled")));
	}
	
	protected String getCosEndPoint() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.endPoint");
	}
	
	protected String getCosApiKey() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.apiKey");
	}
	
	protected String getCosServiceCRN() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.serviceCRN");
	}
	
	protected String getCosBucketName() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.bucketName");
	}
	
	protected String getCosBucketLocation() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.bucketLocation");
	}
	
	protected String getSCISTenantId() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.tenantId");
	}
	
	protected boolean saveCOSDataToFile() {
		return ((Boolean)Boolean.valueOf((String)getProperty("speedment.consumer.cloudobjectstorage.saveToFile")));
	}

	protected String getCosLocalFileLocation() {
		return (String) getProperty("speedment.consumer.cloudobjectstorage.fileLocation");
	}
	
	/*
	public static void main(String[] args)
	{
		Properties	kafkaProps = initializeKafkaProperties();
		Consumer consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new StringDeserializer());

		consumer.subscribe(Collections.singletonList("speedment-topic"));
		logger.info ("Listening...");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                logger.info ("Key = " + (String)record.key() +  " Value = " + (String)record.value());
            }
            logger.info.print (".");
            consumer.commitSync();
        }
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LiveDataKafkaToDBRunner ldToDBRunner;
		Runtime.getRuntime().addShutdownHook(new Thread(ldToDBRunner = new LiveDataKafkaToDBRunner()));
		
		final Thread mainThread = Thread.currentThread();
		
		ldToDBRunner.run();
		logger.info("Running...");
		try {
			ldToDBRunner.waitForQuitKey();
			logger.info ("Shutting down...");
			ldToDBRunner.shutdown();

			mainThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
		} catch (Exception e) {}
	}
*/

}
	