package com.speedment.livedata.agent;

import com.yantra.interop.japi.YIFCustomApi;
import com.yantra.ycp.japi.util.YCPBaseAgent;

import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import 	java.util.concurrent.TimeUnit;

import org.w3c.dom.Document;

import com.speedment.livedata.encrypter.LiveDataEncrypter;
import com.speedment.livedata.global.LiveDataConsts;
import com.yantra.interop.japi.YIFApi;
import com.yantra.interop.japi.YIFClientFactory;
import com.yantra.interop.japi.YIFCustomApi;
import com.yantra.ycp.japi.util.YCPBaseAgent;
import com.yantra.yfc.core.YFCObject;
import com.yantra.yfc.dom.YFCDocument;
import com.yantra.yfc.dom.YFCElement;
import com.yantra.yfc.dom.YFCNode;
import com.yantra.yfc.dom.YFCNodeList;
import com.yantra.yfc.log.YFCLogCategory;
import com.yantra.yfs.japi.YFSEnvironment;
import com.yantra.ycp.core.YCPContext;
import java.util.Date;
import java.text.DateFormat;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class LiveDataAgentImpl extends YCPBaseAgent implements YIFCustomApi {
		  
	  private 		Hashtable<String, Object>	m_Properties;
	  private		Producer<String, String>	m_KafkaProducer;
	  private		Properties					m_KafkaProperties;
	  private		boolean						m_bInKafkaTransaction;

	  // logger instance
	  private static YFCLogCategory logger = YFCLogCategory.instance(LiveDataAgentImpl.class);

	  public LiveDataAgentImpl() 
	  {

		m_Properties = new Hashtable<String, Object>();
		m_KafkaProducer = null;
		m_bInKafkaTransaction = false;
		m_KafkaProperties = null;
	  }

	  public	Producer <String, String>getKafkaProducer () throws Exception
	  {
		  if (m_KafkaProducer == null)
		  {
			m_KafkaProducer = new KafkaProducer<>(m_KafkaProperties, new StringSerializer(), new StringSerializer());
		  }
		  return m_KafkaProducer;
	  }
	  
	  @SuppressWarnings("rawtypes")
	  public java.util.List getJobs (YFSEnvironment env, Document inXML) throws Exception
	  {
		return getJobs(env, inXML, null);
	  }

	  @SuppressWarnings({ "unchecked", "rawtypes" })
	  public java.util.List getJobs (YFSEnvironment env, Document inXML, Document lastMessageXml) throws Exception
	  {
		  initDefaultPropertiesForAgent (env, inXML);

		  if (IsDebugging())
		  {
			  logger.debug ("Input to getJobs:");
			  logger.debug (YFCDocument.getDocumentFor (inXML).getString());
			  debugProperties();
		  }

		  if (lastMessageXml != null  || IsTaskPendingOrRunning (env, getAgentParameter("TaskId", inXML)))
			  return null;
		  ArrayList lstJobs = new ArrayList();


		  try {
			if (((String)getProperty("TaskId")).contains ("RESET"))
			{
				resetRunningOrPendingJobs (env, YFCDocument.getDocumentFor(inXML).getDocumentElement());
				return null;
			}

			// this API object can be used to call an IBM OMS API like getOrderList() API for example.
			YIFApi api = YIFClientFactory.getInstance().getLocalApi ();
			YFCDocument	docDataExtractConfig = YFCDocument.getDocumentFor(api.executeFlow(env, "CocDataExtractConfig",
											   YFCDocument.getDocumentFor("<DataExtractConfig Action=\"LIST\" TaskId=\""+ getAgentParameter("TaskId", inXML)+"\" />").getDocument()));

			YFCElement	eleDataExtractConfig = docDataExtractConfig.getDocumentElement();
			Iterator	iDataExtractConfig = eleDataExtractConfig.getChildren();

			// create a separate job for each table in the list of tables to synch
			if (Boolean.valueOf(getAgentParameter ("OneTablePerJob", inXML, "N")))
			{
				while (iDataExtractConfig.hasNext())
				{
					@SuppressWarnings("unused")
					YFCDocument	docDataExtrConfig = YFCDocument.createDocument ("DataExtractConfigList");
					YFCElement	eleJobXML = (YFCElement)iDataExtractConfig.next();
					getTableColumnsForJob(env, eleJobXML);

					// carry over getJobs() xml attributes to executeJobs() that we want or need
					loadDefaultPropertiesForJob(eleJobXML);

					// load the job details into the list and we're done
					if (initializeJobInDB (env, eleJobXML, (String)getProperty("TransactionId")))
						lstJobs.add (docDataExtractConfig.getDocument());
				}
			}
			else
			{
				// sort by DataExtractConfigKey so we create the list of tables sorted by the key.  this allows us to
				// specify the order in which tables are extracted (Parent/Child Tables) to maintain referential integrity
				// assuming the receiver is using the data to create a shadow data base
				TreeMap	srtDataExtractConfig = new TreeMap ();
				while (iDataExtractConfig.hasNext())
				{
					YFCElement	eleDataConfig = (YFCElement)iDataExtractConfig.next();
					srtDataExtractConfig.put(eleDataConfig.getAttribute("DataExtractConfigKey"), eleDataConfig);
				}
				YFCDocument	docSortedDataExtractConfig = YFCDocument.createDocument("DataExtractConfigListSorted");
				YFCElement	eleSortedDataExtractConfig = docSortedDataExtractConfig.getDocumentElement();

				// carry over getJobs() xml attributes to executeJobs() that we want or need
				loadDefaultPropertiesForJob(eleSortedDataExtractConfig);
							
				iDataExtractConfig = srtDataExtractConfig.keySet().iterator();
				while (iDataExtractConfig.hasNext())
				{
					String		sDataExtractConfigKey = (String)iDataExtractConfig.next();
					YFCElement	eleJobXML = (YFCElement)srtDataExtractConfig.get(sDataExtractConfigKey);
					getTableColumnsForJob(env, eleJobXML);

					if (initializeJobInDB(env, eleJobXML, (String)getProperty("TransactionId")))
						eleSortedDataExtractConfig.importNode((YFCNode)srtDataExtractConfig.get(sDataExtractConfigKey));
				}

				lstJobs.add (docSortedDataExtractConfig.getDocument());
				if (IsDebugging())
					logger.debug ("Exiting getJobs with " + lstJobs.size() + " Jobs to Execute");
					
			}
		} catch (Exception e) {
			if (IsDebugging())
			{
				logger.info ("Exception in getJobs() Method:");
				logger.info ("Exception Class: " + e.getClass() + ": " + e.getMessage());
			}
			throw new Exception (e.getMessage());
		}
		return lstJobs;
	  }

	  public void executeJob(YFSEnvironment env, Document inXML) throws Exception
	  {
		  String	sStatus = LiveDataConsts.LIVEDATA_FAILED;
		  
		  if (IsDebugging())
		  {
				logger.debug ("Input to executeJobs:");
				logger.debug (YFCDocument.getDocumentFor (inXML).getString());
		  }

/*
		  // Registering a shutdown hook so we can exit cleanly
		  final Thread mainThread = Thread.currentThread();
		  Thread	shutdownThread;
		  Runtime.getRuntime().addShutdownHook(shutdownThread = new Thread() {
				public void run() {
					try {
						mainThread.join();
						logger.info("LiveDataAgent Completed Running Job: " + getProperty ("TaskId"));
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
		  });
*/
		  
		  initDefaultPropertiesForAgent (env, inXML);
		  try {
			DateFormat	dfCurrent = DateFormat.getDateTimeInstance();


			Connection OmsDBConnection = ((YCPContext)env).getDBConnection();
			if (IsDebugging())
			{
				logger.debug (dfCurrent.format(new Date()) + ": OMS DB Connection Successful on Schema " + OmsDBConnection.getSchema());
			}
			// this API object can be used to call an IBM OMS API like getOrderList() API for example.
			YIFApi api = YIFClientFactory.getInstance().getLocalApi ();

			// This section is where we should iterate over all the jobs and stream the table data to the listener
			long	lStopTime = Integer.parseInt((String)getProperty ("SimulatedRunTime"));


			YFCElement	eleDataExtractSortedList = YFCDocument.getDocumentFor(inXML).getDocumentElement();
			Iterator<YFCElement>	iDataExtractConfigs = eleDataExtractSortedList.getChildren();
						
			// initialize the transactions that are about to occur
			if (IsKafkaEnabled())
				getKafkaProducer().initTransactions();
			
			while (iDataExtractConfigs.hasNext())
			{
				YFCElement	eleJobXML = (YFCElement)iDataExtractConfigs.next();
				
				loadDefaultPropertiesForJob(eleJobXML);

				updateJobInDB (env, eleJobXML, LiveDataConsts.LIVEDATA_RUNNING);

				// start the kafka transaction
				if (IsKafkaEnabled())
				{
					getKafkaProducer().beginTransaction();
					setInKafkaTransaction(true);
				}
				sStatus = LiveDataConsts.LIVEDATA_FAILED;

				// do work here
				try	{
					YFCDocument	docJobXML = YFCDocument.getDocumentFor(eleJobXML.toString());
					((YCPContext)env).setUserObject(m_KafkaProducer);
					YFCDocument docJobResultsXML = YFCDocument.getDocumentFor(api.executeFlow(env, "CocDataExtractExecuteJob", docJobXML.getDocument()));
					eleJobXML = docJobResultsXML.getDocumentElement();
					sStatus = eleJobXML.getAttribute("Status");
				 } catch (Exception e) {
						sStatus = eleJobXML.getAttribute(LiveDataConsts.LIVEDATA_ABORTED);
				} finally  {
					// update job status
					updateJobInDB (env, eleJobXML, sStatus);
				}
				if (sStatus.equals (LiveDataConsts.LIVEDATA_ABORTED))
				{
					if (IsKafkaEnabled())
						getKafkaProducer().abortTransaction();
				     break;
				}
				else if (sStatus.equals(LiveDataConsts.LIVEDATA_SUCCESS))
				{
					if (IsKafkaEnabled())
						getKafkaProducer().commitTransaction();
				}
				else
					break;
			}
			if (IsKafkaEnabled())
				getKafkaProducer().close();
			if (IsDebugging())
				logger.debug ("Exiting executeJobs.  Status=" + sStatus);

		} catch (Exception e) {
			if (IsKafkaEnabled () && m_KafkaProducer != null)
			{
				if (getInKafkaTransaction())
					getKafkaProducer().abortTransaction();
				getKafkaProducer().close();
			}
			// attempt to clean up pending jobs 
			cleanupPendingJobs (env);
			
			if (IsDebugging())
			{
				logger.info ("Exception in executeJobs() Method:");
				logger.info ("Exception Class = " + e.getClass() +" " + e.getMessage());
			}
			throw new Exception (e.getMessage());
		}
		return;
	  }

	  @SuppressWarnings("unchecked")
	  public	Document	executeDataExtractJobImpl (YFSEnvironment env, Document inXML) throws Exception
	  {
		YFCDocument	docJobXML = YFCDocument.getDocumentFor(inXML);
		YFCElement	eleJobXML = docJobXML.getDocumentElement();
		long		lStopTime = eleJobXML.getLongAttribute("SimulatedRunTime");
		Timestamp	tsLastRunAt = new Timestamp(eleJobXML.getLongAttribute("LastRunAt"));
		Timestamp	tsRunUntil = new Timestamp (eleJobXML.getLongAttribute("RunUntil"));
		Timestamp	tsRunTime = new Timestamp (eleJobXML.getLongAttribute("RunUntil") - eleJobXML.getLongAttribute("LastRecordModifiedTS"));
		
		Timestamp	tsLastRecordModifiedTS = new Timestamp (eleJobXML.getLongAttribute("LastRecordModifiedTS"));
		DateFormat	dfCurrent = DateFormat.getDateTimeInstance();
		String		sCurrentDateTime = dfCurrent.format(new Date());
		Connection	conDstDB = null;

		/*
		 * MANDATORY that any implementation catch any exception here and mark the job failed if it needs to be re-tried
		 */
		Producer<String, String> kafkaProducer = (Producer<String, String>) ((YCPContext)env).getUserObject();
		try {
			// load default properties for job
			initDefaultPropertiesForAgent (env, inXML);
			if (IsDBEnabled())
				conDstDB = getDSTConnection();

			if (IsDebugging())
			{
				logger.debug (dfCurrent.format(new Date()) + ": Executing Job Impl - Input XML:");
				logger.debug (docJobXML.getString());

				logger.info (sCurrentDateTime + ":           Executing Task: " + eleJobXML.getAttribute("TaskId"));
				logger.info (sCurrentDateTime + ":               Table Name: " + eleJobXML.getAttribute("TableName"));
				logger.info (sCurrentDateTime + ":              Last Run At: " + tsLastRunAt.toString());
				logger.info (sCurrentDateTime + ":  Last Record Modified TS: " + tsLastRecordModifiedTS.toString());
				logger.info (sCurrentDateTime + ": Until Record Modified TS: " + tsRunUntil.toString());
				logger.info (sCurrentDateTime + ":  Calculated Run Duration: " + convertMillisecondsToString((double)tsRunTime.getTime()));
				logger.info (sCurrentDateTime + ":             KafakEnabled: " + getProperty ("IsKafkaEnabled"));
				logger.info (sCurrentDateTime + ":                DBEnabled: " + getProperty ("IsDBEnabled"));
				if (lStopTime != 0)
					logger.info (sCurrentDateTime + ":         Simulate Working: Exporting Table " + eleJobXML.getAttribute("TableName") + " for " + lStopTime + " Seconds");
			}

			Connection	conDB = ((YCPContext)env).getDBConnection();
			Timestamp	tsBetweenStart = new Timestamp (tsLastRecordModifiedTS.getTime());
			Timestamp	tsBetweenEnd   = new Timestamp (tsRunUntil.getTime());
		
			String		sTableColumns = eleJobXML.getAttribute("Columns");
			String		sTableName = eleJobXML.getAttribute("TableName");
			
			String		sHeaderToSend = LiveDataConsts.LIVEDATA_BEGINTABLE_IDENTIFIER + "," + eleJobXML.getAttribute("DataExtractConfigKey") + "," + sTableName + "," + sTableColumns;
			// send the table information first (DataExractConfigKey, Table, Columns)
			if (IsKafkaEnabled())
				kafkaProducer.send(new ProducerRecord<>(eleJobXML.getAttribute("speedment.producer.kafka.topic"), Long.toString(System.currentTimeMillis()), sHeaderToSend));

			StringBuilder sSQL = new StringBuilder("SELECT " + sTableColumns + ",MODIFYTS FROM " + sTableName);
			sSQL.append(" WHERE MODIFYTS BETWEEN '" + tsBetweenStart.toString() + "' AND '" + tsBetweenEnd.toString() + "' ORDER BY MODIFYTS");
			if (!YFCObject.isVoid(getProperty ("FetchLimit")))
				sSQL.append(" FETCH FIRST " + getProperty("FetchLimit") + " ROWS ONLY");
			if (IsDebugging())
			{
				logger.debug (sCurrentDateTime + ":          Executing Query:" + sSQL);
			}
			PreparedStatement ps = conDB.prepareStatement(sSQL.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet	rsResults = ps.executeQuery();

			List<String> lstColumns = Arrays.asList(sTableColumns.split("\\s*,\\s*"));
			List<String> lstColumnsToEncode = Arrays.asList(((String)getProperty ("URLEncodedColumns")).split("\\s*,\\s*"));
			long	lTotalRecords = 0;
			if (rsResults.next())
			{
				do {
					// assume the first column of every table is the key to the record
					String			sProducerKeyName = lstColumns.get(0);
					String			sProducerKeyValue = rsResults.getString(1).trim();
					StringBuilder	sRecordToSend = new StringBuilder();
				    for(int i = 1; i <= lstColumns.size(); i++)
				    {
				    	String sColumnName = lstColumns.get(i-1);
				    	String sColumnToSend = rsResults.getString(i);
				        sRecordToSend.append("\"");
				    	if (!YFCObject.isNull(sColumnToSend))
				    	{
				    		// description columns should be URL Encoded to ensure they don't contain embedded quotes
				    		if (lstColumnsToEncode.contains(sColumnName))
				    			sRecordToSend.append(URLEncoder.encode(sColumnToSend.toString(), "UTF-8"));
				    		else
				    			sRecordToSend.append(sColumnToSend.trim());
				    	}
				        sRecordToSend.append("\"");
				        if(i < lstColumns.size())
				            sRecordToSend.append(",");
				    }
					tsLastRecordModifiedTS = rsResults.getTimestamp ("MODIFYTS");
					if (IsDebugging())
					{
						logger.debug (sCurrentDateTime + ":              Data Record:" + sRecordToSend);
					}
					if (IsKafkaEnabled ())
					{
						sProducerKeyValue= sTableName + "-" + sProducerKeyValue;
						kafkaProducer.send(new ProducerRecord<>(eleJobXML.getAttribute("speedment.producer.kafka.topic"), sProducerKeyValue, sRecordToSend.toString()));
					}
					if (IsDBEnabled())
						manageDstDataRecord (conDstDB, eleJobXML, sProducerKeyName, sProducerKeyValue, sRecordToSend.toString());
						
					lTotalRecords++;
				} while (rsResults.next());
			}
			ps.close();
			rsResults.close();
			String sFooterToSend = LiveDataConsts.LIVEDATA_ENDTABLE_IDENTIFIER + "," + sTableName + "," + lTotalRecords;

			if (IsKafkaEnabled())
				kafkaProducer.send(new ProducerRecord<>(eleJobXML.getAttribute("speedment.producer.kafka.topic"), Long.toString(System.currentTimeMillis()), sFooterToSend));
			if (IsDBEnabled())
				conDstDB.commit();
			
			/*  This Code is for Simulation Purposes In case you want to simulate a job running without actually doing anything
			 * 
			 *
			 *
			 */
			try {
				if (lStopTime != 0)
					TimeUnit.SECONDS.sleep(lStopTime);
			} catch (InterruptedException eIgnore) {}
			
			 eleJobXML.setAttribute("TotalRecords", new Long(lTotalRecords).toString());
			 eleJobXML.setAttribute ("LastRecordModifiedTS", Long.toString(tsLastRecordModifiedTS.getTime()));
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_SUCCESS);
		 } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
		     // We can't recover from these exceptions, so our only option is to close the producer and exit.
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_FAILED);
			 if (IsDebugging())
				 logger.info (e.getMessage());
		 } catch (KafkaException e) {
		     // For all other exceptions, just abort the transaction and try again.
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_ABORTED);
		 } catch (NullPointerException e) {
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_ABORTED);
			 if (IsDebugging())
				 logger.info ("A NullPointerException Occured in executeDataExtractJobImpl");
		 } catch (Exception e) {
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_FAILED);
			 if (IsDebugging())
				 logger.info ("An Exception Occured in executeDataExtractJobImpl" + e.getMessage() + e.getSuppressed().toString());
		 } finally {
			 if (IsDBEnabled() && conDstDB != null)
				 conDstDB.close();
		 }
		/*
		 * END of MANDATORY code
		 */

		if (IsDebugging())
		{
			/*  This Code is for Simulation Purposes In case you want to simulate a job running without actually doing anything
			 * 
			 */
			if (lStopTime != 0)
			{
				sCurrentDateTime = dfCurrent.format(new Date());
				logger.info (sCurrentDateTime + ":     Simulate Working: Exporting Table " + eleJobXML.getAttribute("TableName") + " Completed.");
			}

			logger.info (sCurrentDateTime + ":    Completion Status: " + eleJobXML.getAttribute("Status") + " For TaskID " +eleJobXML.getAttribute("TaskId"));
		}

		return (docJobXML.getDocument());
	  }

	  @SuppressWarnings("unused")
	  protected void	manageDstDataRecord (Connection conDstDB, YFCElement eleJobXML, String sRecordKeyName, String sRecordKeyValue, String sRecordToInsertOrModify) throws Exception
	  {
		  try {
			// now create a connection to the destination database
			String				sColumns = eleJobXML.getAttribute("Columns");
			String				sTableName = eleJobXML.getAttribute("TableName");
			String				sSQL = "SELECT " + sColumns + " FROM " + sTableName + " WHERE " + sRecordKeyName + "=" + sRecordKeyValue + " FOR UPDATE";
			PreparedStatement	ps = conDstDB.prepareStatement(sSQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet			rsResults = ps.executeQuery();
			List<String> 		lstColumns = Arrays.asList(sColumns.split("\\s*,\\s*"));
			List<String>		lstValues = Arrays.asList(sRecordToInsertOrModify.split("\\s*,\\s*"));
			
			// perform an update
			if (rsResults.next())
			{
				int		iColumn = 1;
				boolean	bUpdateRow  = false;
				for (String sColumn : lstColumns)
				{
					String sCurrentValue = rsResults.getString (iColumn);
					String sNewValue = lstValues.get (iColumn-1);
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
				for (String sColumn : lstColumns)
				{
					String sNewValue = lstValues.get (iColumn-1);
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
	  
	  private	String	convertMillisecondsToString(double dblMilliseconds)
	  {
		double	dblHrs = Math.floor(dblMilliseconds/1000/60/60);
		double	dblMins = Math.floor((dblMilliseconds/1000/60/60 - dblHrs)*60);
		double  dblSecs = Math.floor(((dblMilliseconds/1000/60/60 - dblHrs)*60 - dblMins)*60);
		String	sHrs  = dblHrs  < 10 ? "0" + new Long((long)dblHrs).toString()  : new Long((long)dblHrs).toString();
		String	sMins = dblMins < 10 ? "0" + new Long((long)dblMins).toString() : new Long((long)dblMins).toString();
		String	sSecs = dblSecs < 10 ? "0" + new Long((long)dblSecs).toString() : new Long((long)dblSecs).toString();
		return (sHrs + ":" + sMins + ":" + sSecs);
	  }

	  protected Properties initializeKafkaProperties (YFSEnvironment env, String sTaskId) throws Exception
	  {
		Properties propCustomerOverrides = new Properties();		
		String		sKafkaPropPrefix = "speedment.producer.kafka.";
	
		logger.debug("Entering initializeKafkaProperties");

		// first we attempt to load properties from the DB 
		propCustomerOverrides.putAll(getDBPropertiesStartingWith(env, sKafkaPropPrefix));
		Enumeration<?>	enumPropNames = propCustomerOverrides.propertyNames();
		// if none of the kafka properties were found in the database
		if (!enumPropNames.hasMoreElements())
		{
			// load properties from customer_overrides.properties
			propCustomerOverrides.load(getClass().getResourceAsStream(LiveDataConsts.LIVEDATA_CUSTOMER_OVERRIDES));
			enumPropNames = propCustomerOverrides.propertyNames();
			logger.debug ("USING PROPERTIES FROM CUSTOMER_OVERRIDES");
			debugProperties(propCustomerOverrides);
		}

		m_KafkaProperties = new Properties();
		while (enumPropNames.hasMoreElements())
		{
			String	sPropName = (String)enumPropNames.nextElement();
			if (sPropName.contains(sKafkaPropPrefix))
			{
				String sSystemPropName = sPropName;
				
				// strip off the category "yfs."
				if (sPropName.startsWith("yfs."))
					sSystemPropName = sPropName.substring(4);
				
				// add kafka properties to the properties used for kafka connections
				String	sSystemPropValue;
				if (!YFCObject.isVoid(sSystemPropValue = getSystemParameter (env, sSystemPropName, sSystemPropName)))
				{
					// enabled and topic are not a real Kafka properties so don't add them
					if (!(sSystemPropName.equals("speedment.producer.kafka.enabled")
					||    sSystemPropName.equals("speedment.producer.kafka.topic")))
						m_KafkaProperties.setProperty(sPropName.substring(sPropName.indexOf(".kafka.")+7), sSystemPropValue);
				}
			}
		}
		// set transactional id to the task id
		m_KafkaProperties.setProperty("transactional.id", sTaskId);
		if (IsDebugging())
		{
			logger.debug ("KAFKA PROPERTIES USED TO CONNECT TO KAFKA TOPIC: ");
			debugProperties(m_KafkaProperties);
		}

		return m_KafkaProperties;
	  }

	@SuppressWarnings("rawtypes")
	private Properties getDBPropertiesStartingWith(YFSEnvironment env, String sPropPrefix) throws Exception
	{
		logger.debug ("Entering getDBPropertiesStartingWith");

		YFCDocument	docGetPropertyMetadataList = YFCDocument.getDocumentFor("<PropertyMetadata Category=\"yfs\" BasePropertyName=\"" + sPropPrefix + "\" BasePropertyNameQryType=\"FLIKE\"/>");
		YIFApi api = YIFClientFactory.getInstance().getLocalApi ();
		Properties props = new Properties();
		
		logger.debug("Input to getPropertyMetadataList");
		logger.debug(docGetPropertyMetadataList.getString());
		
		docGetPropertyMetadataList = YFCDocument.getDocumentFor(api.getPropertyMetadataList(env, docGetPropertyMetadataList.getDocument()));

		logger.debug("Output from getPropertyMetadataList");
		logger.debug(docGetPropertyMetadataList.getString());
		
		YFCElement	elePropertyMetadataList = docGetPropertyMetadataList.getDocumentElement();
		YFCNodeList<YFCElement> eleProperties = elePropertyMetadataList.getElementsByTagName("Property");
		Iterator 	iProperties = eleProperties.iterator();
		if (iProperties.hasNext())
		{
			while (iProperties.hasNext())
			{
				YFCElement	eleProperty = (YFCElement) iProperties.next();
				props.setProperty(eleProperty.getAttribute("BasePropertyName"), eleProperty.getAttribute("PropertyValue"));
			}
			logger.debug ("KAFKA PRODUCER PROPERTIES FOUND IN DB");
			debugProperties (props);
		}
		else
		{
			logger.debug ("NO KAFKA PRODUCER PROPERTIES FOUND IN DB");
		}
		// return properties found
		return props;
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
			logger.debug (sPropName + "=" + sPropValue);
		}
	  }

	  protected boolean	initializeJobInDB (YFSEnvironment env, YFCElement eleJobXML, String sTransactionId) throws Exception
	  {
		boolean	bJobUpdatedInDB = true;
		try {
			Connection	conDB = ((YCPContext)env).getDBConnection();
			String sSql = "SELECT DATA_EXTR_JOB_KEY, DATA_EXTR_CFG_KEY, STATUS, STARTTS, ENDTS, RUN_AT, CREATEPROGID, MODIFYPROGID  FROM YFS_DATA_EXTR_JOB WHERE DATA_EXTR_JOB_KEY='"+ eleJobXML.getAttribute("DataExtractConfigKey")+"' FOR UPDATE";
			PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet	rsJobDetail = ps.executeQuery();
			String		sStatus;

			long		lCurrentTime = System.currentTimeMillis();
			Timestamp	tsCurrentTime = new Timestamp(lCurrentTime);

			// if we already have a job record we need to update it
			if (rsJobDetail.next())
			{
				sStatus = rsJobDetail.getString("STATUS");
				if (IsTerminalStatus(sStatus))
				{
					sStatus = LiveDataConsts.LIVEDATA_PENDING;
					rsJobDetail.updateString("STATUS", sStatus);
					rsJobDetail.updateTimestamp("STARTTS", tsCurrentTime);
					if (sStatus.equals(LiveDataConsts.LIVEDATA_SUCCESS))
						rsJobDetail.updateTimestamp("RUN_AT", rsJobDetail.getTimestamp("STARTTS"));
					rsJobDetail.updateTimestamp("ENDTS", tsCurrentTime);
					rsJobDetail.updateRow();

				}
				else if (sStatus.equals(LiveDataConsts.LIVEDATA_PENDING) || sStatus.equals(LiveDataConsts.LIVEDATA_FIRSTRUN_PENDING) || sStatus.equals(LiveDataConsts.LIVEDATA_RUNNING))
					bJobUpdatedInDB = false;
			}
			else
			{
				sStatus = LiveDataConsts.LIVEDATA_FIRSTRUN_PENDING;
				rsJobDetail.moveToInsertRow();
				rsJobDetail.updateString ("DATA_EXTR_JOB_KEY", eleJobXML.getAttribute("DataExtractConfigKey"));
				rsJobDetail.updateString ("DATA_EXTR_CFG_KEY", eleJobXML.getAttribute("DataExtractConfigKey"));
				rsJobDetail.updateString ("STATUS", sStatus);
				rsJobDetail.updateTimestamp("STARTTS", tsCurrentTime);
				rsJobDetail.updateTimestamp("RUN_AT", new Timestamp(lCurrentTime - eleJobXML.getLongAttribute("FirstRunExtractInDays") * 24L * 60L * 60L * 1000L));
				rsJobDetail.updateTimestamp("ENDTS", tsCurrentTime);
				rsJobDetail.updateString("CREATEPROGID", sTransactionId);
				rsJobDetail.updateString("MODIFYPROGID", sTransactionId);
				rsJobDetail.insertRow();
				rsJobDetail.moveToCurrentRow();
			}
			ps.close();
			rsJobDetail.close();

			// update the corresponding data extract config record status
			updateDataExtractConfigStatus(env, eleJobXML, sStatus);

			// calculate the job's run until ModifiedTS
			calcJobRunUntil (eleJobXML);

			conDB.commit();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return bJobUpdatedInDB;
	  }


	  protected void	updateJobInDB (YFSEnvironment env, YFCElement eleJobXML, String sStatus) throws Exception
	  {
		try {
			Connection	conDB = ((YCPContext)env).getDBConnection();
			String sSql = "SELECT DATA_EXTR_JOB_KEY, DATA_EXTR_CFG_KEY, STATUS, STARTTS, ENDTS, RUN_AT FROM YFS_DATA_EXTR_JOB WHERE DATA_EXTR_JOB_KEY='"+ eleJobXML.getAttribute("DataExtractConfigKey") + "' FOR UPDATE";
			PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet	rsJobDetail = ps.executeQuery();

			long		lCurrentTime = System.currentTimeMillis();
			Timestamp	tsCurrentTime = new Timestamp(lCurrentTime);

			// if we already have a job record we need to update it
			if (rsJobDetail.next())
			{
				// update the status of the job
				rsJobDetail.updateString("STATUS", sStatus);

				// if job status is running then job update start time stamp
				if (sStatus.equals(LiveDataConsts.LIVEDATA_RUNNING))
					rsJobDetail.updateTimestamp("STARTTS", tsCurrentTime);

				// if the job was not successful we want it to run the next time beginning at the old RunAt date/time
				if (sStatus.equals(LiveDataConsts.LIVEDATA_SUCCESS))
					rsJobDetail.updateTimestamp("RUN_AT", rsJobDetail.getTimestamp("STARTTS"));

				rsJobDetail.updateTimestamp("ENDTS", tsCurrentTime);
				rsJobDetail.updateRow();

				// pass back the last run at time to job
				Timestamp tsNextAt = rsJobDetail.getTimestamp("RUN_AT");
				eleJobXML.setAttribute("LastRunAt", tsNextAt.getTime());
			}

			ps.close();
			rsJobDetail.close();

			// update the corresponding data extract config record status
			updateDataExtractConfigStatus(env, eleJobXML, sStatus);
			conDB.commit();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

	  }

	  protected void		updateDataExtractConfigStatus (YFSEnvironment env, YFCElement eleJobXML, String sStatus) throws Exception
	  {
		try {
			Connection	conDB = ((YCPContext)env).getDBConnection();
			String sSql = "SELECT DATA_EXTR_CFG_KEY, TASK_ID, RUNNING_STATUS, FREQUENCY_HRS, FREQUENCY_MINS, NEXT_RUN, NEXT_STARTTS FROM YFS_DATA_EXTR_CFG WHERE DATA_EXTR_CFG_KEY='"+ eleJobXML.getAttribute("DataExtractConfigKey") + "' FOR UPDATE";
			PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet	rsCfgDetail = ps.executeQuery();
			long		lCurrentTime = System.currentTimeMillis();

			if (rsCfgDetail.next())
			{
				// update the status of the job
				Timestamp	tsLastRecordModifiedTS = rsCfgDetail.getTimestamp("NEXT_STARTTS");

				// update next run based on frequency configuration
				long		lNextInterval = lCurrentTime;
				lNextInterval += rsCfgDetail.getInt("FREQUENCY_HRS") * 60L * 60L * 1000L;
				lNextInterval += rsCfgDetail.getInt("FREQUENCY_MINS") * 60L * 1000L;
				rsCfgDetail.updateTimestamp ("NEXT_RUN", new Timestamp (lNextInterval));

				// due to a bug in the service that reads the data extract config that doesn't return the FrequencyInMins
				// we set it into the job record so we'll have it
				eleJobXML.setAttribute("FrequencyInMins", rsCfgDetail.getInt("FREQUENCY_MINS"));

				// save last record modified time stamp if passed, otherwise return the current time stamp in job xml
				if (!YFCObject.isVoid(eleJobXML.getAttribute("LastRecordModifiedTS")) && sStatus.equals(LiveDataConsts.LIVEDATA_SUCCESS))
					rsCfgDetail.updateTimestamp ("NEXT_STARTTS", new Timestamp (eleJobXML.getLongAttribute("LastRecordModifiedTS")));
				else if (!YFCObject.isNull(tsLastRecordModifiedTS))
					eleJobXML.setAttribute("LastRecordModifiedTS", tsLastRecordModifiedTS.getTime());
				else
					eleJobXML.setAttribute("LastRecordModifiedTS", eleJobXML.getAttribute("LastRunAt"));
				rsCfgDetail.updateString("RUNNING_STATUS", sStatus);
				rsCfgDetail.updateRow();
			}
			ps.close();
			rsCfgDetail.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return;
	  }

	  protected	void	calcJobRunUntil (YFCElement eleJobXML)
	  {
		long	lCurrentTime = System.currentTimeMillis() - (Long.valueOf((String)getProperty("MinAtRestSeconds")) * 1000L);
		long	lNewRunUntil;

		long	lFrequencyInHours = eleJobXML.getLongAttribute("FrequencyInHours");
		long	lFrequencyInMins = eleJobXML.getLongAttribute("FrequencyInMins");

		long	lRunUntil = eleJobXML.getLongAttribute("LastRecordModifiedTS");
		if (lRunUntil == 0)
			lRunUntil = eleJobXML.getLongAttribute("LastRunAt");

		// make sure frequencies are not zero
		if (lFrequencyInMins > 0 || lFrequencyInMins > 0)
		{
			do {
				// calculate the run until time which will be upper range of the ModifiedTS used.
				// Using the frequency values will ensure we're always processing those records modified up to
				// but not past a given interval.  
				
				// e.g. if configured interval is 5 minutes the upper range
				// will be a multiple of 5 minutes (i.e. 5, 10, 15) from NOW depending on the last time the job was
				// last run.
				lNewRunUntil = lRunUntil + (lFrequencyInHours * 60L * 60L * 1000L) + (lFrequencyInMins * 60L * 1000L);

				if (lNewRunUntil <= lCurrentTime)
					lRunUntil = lNewRunUntil;
			} while (lNewRunUntil <= lCurrentTime);
		}
		// to allow records to commit we will decrease the until run time by one second
		eleJobXML.setAttribute ("RunUntil", Long.toString(lRunUntil));
	  }
	  
	  protected boolean IsAnyJobPendingOrRunning (YFSEnvironment env) throws Exception
	  {
		  	return IsTaskPendingOrRunning (env, null);
	  }
	  
	  protected boolean	IsTaskPendingOrRunning (YFSEnvironment env, String sTaskId) throws Exception
	  {
		boolean		bRet = false;
		long		lCurrentTime = System.currentTimeMillis();

		try {			
			Connection	conDB = ((YCPContext)env).getDBConnection();
			String sSQL = "SELECT DATA_EXTR_CFG_KEY, TASK_ID, RUNNING_STATUS, NEXT_RUN FROM YFS_DATA_EXTR_CFG";
			if (sTaskId != null)
				sSQL = sSQL +  " WHERE TASK_ID='"+ sTaskId + "'";
			PreparedStatement ps = conDB.prepareStatement(sSQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet	rsCfgDetail = ps.executeQuery();
			
			
			// Look across all the jobs for a given task id to see if any are pending or running
			DateFormat	dfCurrent = DateFormat.getDateTimeInstance();
			String	sCurrentDateTime = dfCurrent.format(new Date());
			if (rsCfgDetail.next())
			{
				do {
					String sStatus = rsCfgDetail.getString("RUNNING_STATUS");
					Timestamp		tsNextRunAt = rsCfgDetail.getTimestamp("NEXT_RUN");

					if (!IsTerminalStatus (sStatus)
					|| (!YFCObject.isNull(tsNextRunAt) && tsNextRunAt.getTime() > lCurrentTime))
					{
						if (IsDebugging() && sTaskId != null)
						{
							logger.info (sCurrentDateTime + ": Task Is Already Pending or Running, or Is Before Next Scheduled Start Date: " + sTaskId);
						}
						else if (IsDebugging())
							logger.info((sCurrentDateTime + ": A Pending Task has been Reset Due to Shutdown - " + rsCfgDetail.getString("TASK_ID")));
						bRet=true;
						break;
					}
				} while (rsCfgDetail.next());
			}
			else
			{
				if (IsDebugging() && !sTaskId.contains("RESET") && sTaskId != null)
				 logger.info (sCurrentDateTime + ": No Task Config Records Found for Task: " + sTaskId);
			}
			ps.close();
			rsCfgDetail.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return bRet;
	  }

	  protected void resetRunningOrPendingJobs (YFSEnvironment env, YFCElement eleJobXML) throws Exception
	  {
		long						lNoJobsReset = 0;
		Connection					conDB = ((YCPContext)env).getDBConnection();
		String						sTasksToResetForFirstRun = (String)getProperty ("TasksToResetForFirstRun");
	    List<String>				lstTaskToResetForFirstRun = Arrays.asList(sTasksToResetForFirstRun.split("\\s*,\\s*"));
		List<String>				lstTablesToResetForFirstRun = new ArrayList<String>();

		Hashtable<String, String>	htTableColumsAndLengths = new Hashtable<String, String> ();
		String 						sDBAction = ((String)getProperty ("DBAction")).toUpperCase();

		setInKafkaTransaction(false);
		eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_SUCCESS);
		
		// PHASE I = Update YFS_DATA_EXTR_CFG Table RUNNING STATUSES and NEXT_RUN/NEXT_STARTTS
		try {
			String sSql = "SELECT DATA_EXTR_CFG_KEY, TASK_ID, TABLE_NAME, RUNNING_STATUS, NEXT_RUN, NEXT_STARTTS FROM YFS_DATA_EXTR_CFG FOR UPDATE";
			PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet	rsCfgDetail = ps.executeQuery();

			while (rsCfgDetail.next())
			{
				String	sCfgDetailKey = rsCfgDetail.getString("DATA_EXTR_CFG_KEY");
				String	sTaskId = rsCfgDetail.getString("TASK_ID");
				String	sStatus = rsCfgDetail.getString("RUNNING_STATUS");
				String	sTableName = rsCfgDetail.getString("TABLE_NAME");
				boolean	bUpdateRow = false;
				if(!IsTerminalStatus (sStatus))
				{
					rsCfgDetail.updateString ("RUNNING_STATUS", LiveDataConsts.LIVEDATA_RESET);
					bUpdateRow = true;
				}
				if (lstTaskToResetForFirstRun.contains(sTaskId))
				{
					rsCfgDetail.updateNull ("NEXT_RUN");
					rsCfgDetail.updateNull("NEXT_STARTTS");
					bUpdateRow = true;
				}
				if (bUpdateRow)
				{
					rsCfgDetail.updateRow();
					lNoJobsReset++;
				}
				
				if (lstTaskToResetForFirstRun.contains(sTaskId))
				{
					String						sTableColumns = getTableColumnsForJob (env, sCfgDetailKey);
					boolean			 			bNewColumnLengthCombo = false;
					java.util.List<String>		lstTableColumns = Arrays.asList(sTableColumns.split("\\s*,\\s*"));
					StringBuilder				sTableColumnsAndLengths = new StringBuilder();

					for (String sTableColumn : lstTableColumns)
					{
						if (bNewColumnLengthCombo)
							sTableColumnsAndLengths.append(',');
						sTableColumnsAndLengths.append(sTableColumn + "," + Integer.toString(getSuggestedColumnWidth (conDB, sTableName, sTableColumn)));
						bNewColumnLengthCombo = true;
					}
					
					// add table columns & lengths to the table columns hashtable for use in DBAction=CREATE
					htTableColumsAndLengths.put(sTableName, sTableColumnsAndLengths.toString());
					lstTablesToResetForFirstRun.add(sTableName);
				}
			}
			ps.close();
			rsCfgDetail.close();
		} catch (Exception e) {
			eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_ABORTED);
			if (IsDebugging())
			{
				logger.info ("Exception in  PHASE I resetRunningOrPendingJobs: " + e.getClass() + " " + e.getMessage());
			}
		} finally {

		}
		
		// PHASE II = DELETE Tasks FROM YFS_DATA_EXTR_JOB Table
		try {
			if (lstTaskToResetForFirstRun.size() > 0)
			{
				for (String sTaskToResetForFirstRun : lstTaskToResetForFirstRun)
				{
					String sSql = "DELETE FROM YFS_DATA_EXTR_JOB WHERE DATA_EXTR_CFG_KEY LIKE'" + sTaskToResetForFirstRun + "%'";
					PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
					ps.execute();
					ps.close();
				}
			}		
		} catch (Exception e) {
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_FAILED);
			 if (IsDebugging())
				 logger.info ("Exception in PHASE II resetRunningOrPendingJobs: " + e.getClass() + " " + e.getMessage());
		}
		
		// PHASE III = Update Remaining YFS_DATA_EXTR_JOB Table STATUS
		try {
			String sSql = "SELECT DATA_EXTR_JOB_KEY, DATA_EXTR_CFG_KEY, STATUS FROM YFS_DATA_EXTR_JOB FOR UPDATE";
			PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet	rsJobDetail = ps.executeQuery();

			// if we already have a job record we need to update it
			while (rsJobDetail.next())
			{
				// update the status of the job
				String sStatus = rsJobDetail.getString("STATUS");
				if(sStatus.equals(LiveDataConsts.LIVEDATA_PENDING) || sStatus.equals (LiveDataConsts.LIVEDATA_RUNNING))
				{
					rsJobDetail.updateString("STATUS", LiveDataConsts.LIVEDATA_ABORTED);
					rsJobDetail.updateRow();
				}
			}
			ps.close();
			rsJobDetail.close();
		} catch (Exception e) {
			 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_FAILED);
			 if (IsDebugging())
				 logger.info ("Exception in PHASE III resetRunningOrPendingJobs: " + e.getClass() + " " + e.getMessage());
		}
		conDB.commit();

		// PHASE IV = if DBEnabled delete/drop/create tables to be reset
		// if we're directly connect to the Destination DB,  delete or drop any tables based on the reset DBAction requested
		if (IsDBEnabled() && eleJobXML.getAttribute("Status").contentEquals(LiveDataConsts.LIVEDATA_SUCCESS))
		{
		  try {
				conDB = getDSTConnection();

				for (String sTableToResetForFirstRun : lstTablesToResetForFirstRun)
				{
					StringBuilder sSQL = new StringBuilder();
					
					while (!sDBAction.equalsIgnoreCase("NONE")) 
					{
						// DBAction can be one of DELETE, DROP, CREATE, DROPANDCREATE or NONE
						if (sDBAction.equalsIgnoreCase("DELETE"))
						{
							sSQL.append("DELETE FROM " + sTableToResetForFirstRun);
						}
						else if (sDBAction.equalsIgnoreCase("DROP") || sDBAction.equalsIgnoreCase("DROPANDCREATE"))
						{
							sSQL.append("DROP TABLE IF EXISTS " + sTableToResetForFirstRun);
						}
						else if (sDBAction.equalsIgnoreCase("CREATE"))
						{
							sSQL = new StringBuilder(sDBAction + "CREATE TABLE IF NOT EXISTS " + sTableToResetForFirstRun);
							List<String> 		lstColumnDetails = Arrays.asList(htTableColumsAndLengths.get(sTableToResetForFirstRun).split("\\s*,\\s*"));
							sSQL.append(" (");
							Iterator<String>	iColumns = lstColumnDetails.iterator();
							while (iColumns.hasNext())
							{
								String	sColumnName = iColumns.next();
								String	sColumnLength = iColumns.next();
								
								sSQL.append("\"" + sColumnName + "\" TEXT(" + sColumnLength + ")");
								sSQL.append(',');							
							}
							sSQL.append("PRIMARY KEY(" + lstColumnDetails.get(0) + ")");
						}
						if (IsDebugging ())
						{
							logger.debug ("Executing SQL:");
							logger.debug (sSQL);
						}
						Statement stmt = conDB.createStatement();
						try {
							stmt.execute(sSQL.toString());
						} catch (Exception e) {
							sDBAction = "NONE";
					  	     if (IsDebugging())
					  	  		  logger.debug ("Exception Executing SQL in resetRunningOrPendingJobs: " + e.getClass() + e.getMessage());
						} finally {
							stmt.close();
						}
						// if we're dropping and creating do the create next
						if (sDBAction.equalsIgnoreCase("DROPANDCREATE"))
							sDBAction = "CREATE";
						else
							sDBAction = "NONE";
					}
				}
				conDB.commit();
		  } catch (Exception e) {
			  eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_FAILED);
			  if (IsDebugging())
				  	logger.info ("Exception in PHASE IV resetRunningOrPendingJobs: " + e.getClass() + " " + e.getMessage());
		  }
		}
		// PHASE V = if KafkaEnabled send RESET commands to Kafka Topic
		if (IsKafkaEnabled())
		{
			// send SPEEDMENT-RESET to Kafka
			try {
				getKafkaProducer().initTransactions();
				for (String sSendTableName : lstTablesToResetForFirstRun)
				{
					String sRecordToSend = LiveDataConsts.LIVEDATA_RESET_IDENTIFIER + "," + sDBAction.toUpperCase() + "," + sSendTableName + ","  + htTableColumsAndLengths.get(sSendTableName);

					// send the following reset message SPEEDMENT-RESET,DBACTION,TABLE_NAME,TABLE_COLUMNS_AND_LENGTHS
					getKafkaProducer().beginTransaction();
					setInKafkaTransaction(true);
					getKafkaProducer().send(new ProducerRecord<>((String)getProperty("speedment.producer.kafka.topic"), Long.toString(System.currentTimeMillis()), sRecordToSend));
					getKafkaProducer().commitTransaction();
					setInKafkaTransaction(false);
				}
			} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			     // We can't recover from these exceptions, so our only option is to close the producer and exit.
				 eleJobXML.setAttribute("Status", LiveDataConsts.LIVEDATA_ABORTED);
				  if (IsDebugging())
					  	logger.info ("Exception in PHASE V resetRunningOrPendingJobs: " + e.getClass() + " " + e.getMessage());
			} catch (Exception e) {
			     // For all other exceptions, just abort the transaction and try again.
				if (getInKafkaTransaction())
						getKafkaProducer().abortTransaction();
				  if (IsDebugging())
					  	logger.info ("Exception in PHASE V resetRunningOrPendingJobs: " + e.getClass() + " " + e.getMessage());
			} finally {
				if (!YFCObject.isNull(getKafkaProducer()))
					getKafkaProducer().close();
			}
		}
		
		if (IsDebugging())
		{
			DateFormat	dfCurrent = DateFormat.getDateTimeInstance();
			String	sCurrentDateTime = dfCurrent.format(new Date());

			logger.info (sCurrentDateTime + ": Reset " + lNoJobsReset + " Running or Pending Jobs");
			logger.info (sCurrentDateTime + ": Status = " +eleJobXML.getAttribute("Status"));
		}
		return;
	  }
	  
	  protected	void	cleanupPendingJobs (YFSEnvironment env)
	  {
			Connection					conDB = ((YCPContext)env).getDBConnection();

			// PHASE I = Update YFS_DATA_EXTR_CFG Table RUNNING STATUSES
			try {
				String sSql = "SELECT DATA_EXTR_CFG_KEY, TASK_ID, TABLE_NAME, RUNNING_STATUS, NEXT_RUN, NEXT_STARTTS FROM YFS_DATA_EXTR_CFG FOR UPDATE";
				PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
				ResultSet	rsCfgDetail = ps.executeQuery();

				while (rsCfgDetail.next())
				{
					String	sStatus = rsCfgDetail.getString("RUNNING_STATUS");
					boolean	bUpdateRow = false;
					if(IsPendingStatus (sStatus))
					{
						rsCfgDetail.updateString ("RUNNING_STATUS", LiveDataConsts.LIVEDATA_RESET);
						bUpdateRow = true;
					}
					if (bUpdateRow)
					{
						rsCfgDetail.updateRow();
					}
				}
				ps.close();
				rsCfgDetail.close();
				conDB.commit();
			} catch (Exception e) {
				if (IsDebugging())
				{
					logger.info ("Exception in PHASE I cleanupPendingJobs: " + e.getClass() + " " + e.getMessage());
				}
			}
			
			// PHASE II = Update Remaining YFS_DATA_EXTR_JOB Table STATUS
			try {
				String sSql = "SELECT DATA_EXTR_JOB_KEY, DATA_EXTR_CFG_KEY, STATUS FROM YFS_DATA_EXTR_JOB FOR UPDATE";
				PreparedStatement ps = conDB.prepareStatement(sSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
				ResultSet	rsJobDetail = ps.executeQuery();

				// if we already have a job record we need to update it
				while (rsJobDetail.next())
				{
					// update the status of the job
					String sStatus = rsJobDetail.getString("STATUS");
					if(IsPendingStatus(sStatus))
					{
						rsJobDetail.updateString("STATUS", LiveDataConsts.LIVEDATA_RESET);
						rsJobDetail.updateRow();
					}
				}
				ps.close();
				rsJobDetail.close();
				conDB.commit();
			} catch (Exception e) {
				 if (IsDebugging())
					 logger.info ("Exception in PHASE II cleanupPendingJobs: " + e.getClass() + " " + e.getMessage());
			}
	  }
	  
	  protected	String getTableColumnsForJob (YFSEnvironment env, YFCElement eleJobXML) throws Exception
	  {
		  eleJobXML.setAttribute("Columns", getTableColumnsForJob (env, eleJobXML.getAttribute("DataExtractConfigKey")));
		  return (eleJobXML.getAttribute("Columns"));
	  }
	  
	  protected	String getTableColumnsForJob (YFSEnvironment env, String sDataExtractConfigKey) throws Exception
	  {
			YIFApi api = YIFClientFactory.getInstance().getLocalApi ();
			Document	docDataExtractConfig = api.executeFlow(env, "CocDataExtractConfig",
											   YFCDocument.getDocumentFor("<DataExtractConfig Action=\"GET\" DataExtractConfigKey=\""+ sDataExtractConfigKey + "\" />").getDocument());
			YFCElement eleDataExtractConfig = YFCDocument.getDocumentFor(docDataExtractConfig).getDocumentElement();
			if (!YFCObject.isVoid(eleDataExtractConfig = eleDataExtractConfig.getFirstChildElement()))
			{
				String 			sTableColumns = eleDataExtractConfig.getAttribute("Columns");
				List<String>	lstTableColumns = Arrays.asList(sTableColumns.split("\\s*,\\s*"));
				StringBuilder	strTableColumns = new StringBuilder();
				
				// table columns configured with just the single PRIMARY KEY column will force all columns to be extracted
				if (lstTableColumns.size() == 1 && lstTableColumns.get(0).contains("KEY"))
				{
					lstTableColumns = getAllColumnNames (((YCPContext)env).getDBConnection(), eleDataExtractConfig.getAttribute("TableName"));
					boolean		bAppendComma = false;
					for (String sTableColumn : lstTableColumns)
					{
						if (bAppendComma)
							strTableColumns.append(',');
						strTableColumns.append(sTableColumn);
						bAppendComma = true;
					}
				}
				else
					strTableColumns.append(sTableColumns);
				return strTableColumns.toString();
			}
			else
				return "";
	  }

	  protected boolean IsTerminalStatus (String sStatus)
	  {
			return (sStatus.equals(LiveDataConsts.LIVEDATA_SUCCESS) || sStatus.equals(LiveDataConsts.LIVEDATA_FAILED) || sStatus.contentEquals(LiveDataConsts.LIVEDATA_ABORTED) || sStatus.contentEquals(LiveDataConsts.LIVEDATA_RESET) || sStatus.equals("-1"));
	  }

	  protected boolean IsPendingStatus (String sStatus)
	  {
			return (sStatus.equals(LiveDataConsts.LIVEDATA_PENDING) || sStatus.equals(LiveDataConsts.LIVEDATA_FIRSTRUN_PENDING));
	  }
	  
	  protected void initDefaultPropertiesForAgent (YFSEnvironment env, Document inXML) throws Exception
	  {
	 
			getAgentParameter("Debug", inXML, "false");
			getAgentParameter("SimulatedRunTime", inXML, "0");
			getAgentParameter("FetchLimit", inXML, Integer.toString(LiveDataConsts.LIVEDATA_DEFAULT_FETCHLIMIT));
			getAgentParameter("TaskId", inXML);
			getAgentParameter("TransactionId", inXML);
			getAgentParameter("TasksToResetForFirstRun", inXML, "");
			getAgentParameter("DBAction", inXML, "NONE");
			getAgentParameter("MinAtRestSeconds", inXML, "10");
			
			getSystemParameter (env, "speedment.urlencoded.columns", "URLEncodedColumns", "");
			
			if (Boolean.valueOf(getSystemParameter (env, "speedment.producer.kafka.enabled", "IsKafkaEnabled", "false")))
			{
				if (m_KafkaProperties == null)
					initializeKafkaProperties(env, (String)getProperty ("TaskId"));
			}
			
			if (Boolean.valueOf(getSystemParameter (env, "speedment.producer.database.enabled", "IsDBEnabled", "false")))
			{
				getSystemParameter(env, "speedment.producer.database.DstDBServer", "DstDBServer");
				getSystemParameter(env, "speedment.producer.database.DstDBType", "DstDBType", getDstDBType());
				getSystemParameter(env, "speedment.producer.database.DstDBPort", "DstDBPort", getDstDBPort());
				getSystemParameter(env, "speedment.producer.database.DstDBSchema", "DstDBSchema", getDstDBSchema());
				getSystemParameter(env, "speedment.producer.database.DstDBUsername", "DstDBUsername", getDstDBUsername());
				getSystemParameter(env, "speedment.producer.database.DstDBPassword", "DstDBPassword", getDstDBPassword());				
			}
	  }

	  protected void loadDefaultPropertiesForJob (YFCElement eleJobXML) throws Exception
	  {
			Enumeration<String>	enumPropNames = (Enumeration <String>)getProperties().keys();
			while (enumPropNames.hasMoreElements())
			{
				String	sPropName = enumPropNames.nextElement();
				eleJobXML.setAttribute(sPropName, (String)getProperty(sPropName));
			}
	  }
	  
	  protected	int	getSuggestedColumnWidth (Connection conDB, String sTable, String sColumn)
	  {
		  String sSQL = "SELECT " + sColumn + " FROM " + sTable + " FETCH FIRST 1 ROW ONLY";
		  int	 iMaxColLength = 24;
		  try {
			  Statement			stmt = conDB.createStatement();
			  ResultSet			rsMaxLength = stmt.executeQuery(sSQL);
			  if (rsMaxLength.next())
				  iMaxColLength = rsMaxLength.getMetaData().getPrecision(1);

			  stmt.close();
			  rsMaxLength.close();
		  } catch (Exception e) {
			  if (IsDebugging())
				  logger.info ("Exception in getSuggestedColumnWidth: " + e.getClass() + " " + e.getMessage());
	  	  }
		  return iMaxColLength;
	  }

	  protected	java.util.List<String>	getAllColumnNames (Connection conDB, String sTable)
	  {
		  String sSQL = "SELECT * FROM " + sTable + " FETCH FIRST 1 ROW ONLY";
		  List<String> lstColumnNames = new ArrayList<String>();
		  try {
			  Statement			stmt = conDB.createStatement();
			  ResultSet			rsColumns = stmt.executeQuery(sSQL);
			  int				iColumn = 1;
			  
			  if (rsColumns.next())
			  {
				  try {
					  while (iColumn < rsColumns.getMetaData().getColumnCount())
					  	lstColumnNames.add((String)rsColumns.getMetaData().getColumnName(iColumn++));
				  } catch (Exception e) {
					  if (IsDebugging())
						  logger.info ("Exception in getAllColumnNames Iteraterating over Columns: " + e.getClass() + " " + e.getMessage());
				  }
			  }

			  stmt.close();
			  rsColumns.close();
			  if (IsDebugging())
			  {
				  iColumn = 1;
				  logger.info ("getAllColumnNames() method returned " + lstColumnNames.size() + " columns");
				  for (String sColumn : lstColumnNames)
				  {
					  logger.info("Column " + iColumn + "=" + sColumn);
					  iColumn++;
				  }
			  }
		  } catch (Exception e) {
			  if (IsDebugging())
				  logger.info ("Exception in getAllColumnNames: " + e.getClass() + " " + e.getMessage());
	  	  }
		  return lstColumnNames;
	  }

	  protected boolean IsDebugging ()
	  {
		  return ((Boolean)Boolean.valueOf((String)getProperty("Debug")));
	  }
	  
	  protected boolean IsDBEnabled ()
	  {
		  return ((Boolean)Boolean.valueOf((String)getProperty("IsDBEnabled")));
	  }
	  
	  protected boolean IsKafkaEnabled ()
	  {
		  return ((Boolean)Boolean.valueOf((String)getProperty("IsKafkaEnabled")));
	  }


	  protected String getSystemParameter (YFSEnvironment env, String strPropertyName, String strAttrName, String strDefault) throws Exception
	  {
		String		strAttrValue = getSystemParameter (env, strPropertyName, strAttrName);

		if (YFCObject.isVoid(strAttrValue))
		{
			strAttrValue = strDefault;
			Hashtable<String, Object>	mProps = getProperties();
			mProps.put (strAttrName, strAttrValue);
		}
		return strAttrValue;
	  }

	  protected String getSystemParameter (YFSEnvironment env, String strPropertyName, String strAttrName) throws Exception
	  {
		YIFApi		api = YIFClientFactory.getInstance().getLocalApi ();
		YFCDocument	docGetProperty = YFCDocument.getDocumentFor ("<GetProperty Category=\"yfs\" PropertyName=\"" + strPropertyName + "\"/>");
		Document	docProperty = api.getProperty(env, docGetProperty.getDocument());
		String		strAttrValue = null;
		
		if (!YFCObject.isNull(docProperty))
			strAttrValue = docProperty.getDocumentElement().getAttribute("PropertyValue");
		
		if (!YFCObject.isVoid(strAttrValue))
		{
			LiveDataEncrypter ldeEncrypter = new LiveDataEncrypter();
			
			Hashtable<String, Object>	mProps = getProperties();
			mProps.put (strAttrName, ldeEncrypter.decryptIfEncrypted(strAttrValue));
		}
		return strAttrValue;
	  }

	  protected String getAgentParameter (String strAttrName, Document docIn, String strDefault) throws Exception
	  {
		String		strAttrValue = getAgentParameter (strAttrName, docIn);

		if (strAttrValue == null || strAttrValue.length() == 0)
		{
			strAttrValue = strDefault;
			Hashtable<String, Object>	mProps = getProperties();
			mProps.put (strAttrName, strAttrValue);
		}
		return strAttrValue;
	  }

	  protected String getAgentParameter (String strAttrName, Document docIn) throws Exception
	  {
		String strAttrValue = null;
		Hashtable<String, Object>	mProps = getProperties();

		if (mProps != null && mProps.containsKey(strAttrName))
			strAttrValue = (String)mProps.get(strAttrName);

		if (strAttrValue == null && docIn != null)
		{
			strAttrValue = docIn.getDocumentElement().getAttribute(strAttrName);
			if (strAttrValue != null)
				mProps.put(strAttrName, strAttrValue);
		}
		return strAttrValue;
	  }

	  public	void debugProperties ()
	  {
		Enumeration<String>	enumPropNames = m_Properties.keys();
		while (enumPropNames.hasMoreElements())
		{
			String	sPropName = enumPropNames.nextElement();
			logger.debug (sPropName + "="+ m_Properties.get(sPropName));
		}
	  }

	  public	void setProperties (Properties props)
	  {
		Enumeration<?>	enumPropNames = props.propertyNames();
		while (enumPropNames.hasMoreElements())
		{
			String	sPropName = (String)enumPropNames.nextElement();
			m_Properties.put(sPropName, props.getProperty(sPropName));
		}
	  }

	  
	  public void setProperty(String sProp, Object sValue)
	  {
		m_Properties.put(sProp, sValue);
	}

	  protected Object getProperty(String sProp)
	  {
		return m_Properties.get(sProp);
	}

	  protected	Hashtable<String, Object>	getProperties ()
	  {
		return m_Properties;
	  }


		/*
		 *
		 * Connection Method for the Target System
		 */
	  
		protected String getDstDBType(){
			if (!YFCObject.isVoid(getProperty("DstDBType"))){
				return (String) getProperty("DstDBType");
			}
			return "DB2";
		}

		protected String getDstDBServer(){
			if (!YFCObject.isVoid(getProperty("DstDBServer"))){
				return (String) getProperty("DstDBServer");
			}
			return "oms.omfulfillment.com";
		}

		protected String getDstDBPort(){
			if (!YFCObject.isVoid(getProperty("DstDBPort"))){
				return (String) getProperty("DstDBPort");
			}
			return "50000";
		}

		protected String getDSTDatabase(){
			if (!YFCObject.isVoid(getProperty("DSTDatabase"))){
				return (String) getProperty("DSTDatabase");
			}
			return "OMDB";
		}

		protected String getDstDBUsername(){
			if (!YFCObject.isVoid(getProperty("DstDBUsername"))){
				return (String) getProperty("DstDBUsername");
			}
			return "demouser";
		}

		protected String getDstDBPassword(){
			if (!YFCObject.isVoid(getProperty("DstDBPassword"))){
				return (String) getProperty("DstDBPassword");
			}
			return "meeting265bridge";
		}

		protected String getDstDBSchema(){
			if (!YFCObject.isVoid(getProperty("DstDBSchema"))){
				return (String) getProperty("DstDBSchema");
			}
			return "OMDB";
		}

		protected Connection getDSTConnection() throws SQLException, ClassNotFoundException {
			Connection dbConn = getDBConnection(getDstDBType(), getDstDBServer(), getDstDBPort(), getDSTDatabase(), getDstDBUsername(), getDstDBPassword());
			return dbConn;
		}

		private String getDBURL(String _dbType, String _sServer, String _sPort, String _sDatabase){
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

		private String getJDBCDriver(String _dbType){
			if (_dbType.equals("DB2")){
				return "com.ibm.db2.jcc.DB2Driver";
			} else if (_dbType.equals("Oracle")){
				return "oracle.jdbc.driver.OracleDriver";
			} else if (_dbType.equals("sqlite")) {
				return "org.sqlite.JDBC";
			}
			return null;
		}

		private Connection getDBConnection(String _dbType, String _sServer, String _sPort, String _sDatabase, String _sUserName, String _sPassword) throws ClassNotFoundException, SQLException{
			Class.forName(getJDBCDriver(_dbType));
			Connection jdbcConnection = (Connection)DriverManager.getConnection(getDBURL(_dbType, _sServer, _sPort, _sDatabase), _sUserName, _sPassword);
			return jdbcConnection;
		}

		public boolean getInKafkaTransaction() {
			return m_bInKafkaTransaction;
		}

		public void setInKafkaTransaction(boolean bInKafkaTransaction) {
			m_bInKafkaTransaction = bInKafkaTransaction;
		}
}
