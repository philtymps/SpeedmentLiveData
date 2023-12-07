package com.speedment.livedata.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectTagging;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.Tag;
import com.speedment.livedata.data.types.*;
import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class LiveDataKafkaToCOSRunner {
	
	//COS variables
   	private String COS_ENDPOINT = null;
   	private String COS_API_KEY_ID = null;    	
   	private String COS_SERVICE_CRN = null; 
   	private String COS_BUCKET_LOCATION = null;
   	private String COS_BUCKET_NAME = null;   	
   	private String SCIS_TENANT_ID = null;
   	private AmazonS3 cosClient = null;
   	private HashMap<String, String> cosInputMap = null;
   	private HashMap<String, OrderHeader> orderHeaderMp = null;
   	private LiveDataKafkaToDBClient ktdClient = null;
   	private List<OrderLine> orderLinesList = null;
   	private List<Organization> shipNodeList = null;
   	private HashMap<String, PersonInfo>  personInfoMp = null;
   	
   	private Logger logger = Logger.getLogger(LiveDataKafkaToCOSRunner.class);
   	
   	public LiveDataKafkaToCOSRunner(LiveDataKafkaToDBClient ktdInputObj) {
   		ktdClient = ktdInputObj;
   	   	initiateCosVariables();
   	}
   	
    public void publishCOSDataToCloud() throws Exception {	
    	createJsonFromLineData();
    	createJsonFromShipNodeData();
    	if(cosInputMap !=null) {
    		cosInputMap.forEach((k,v) ->
	    		{
					try {
						uploadFileFromText(k, v);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
	    	);
    	}
    	cosInputMap = null;
    	orderHeaderMp = null;
    	orderLinesList = null;
    	shipNodeList = null;
	} 

	public boolean processCOSDataRecord(String sTableName, String sTableColumns, 
			List<String> lstTableColumns, List<String> lstTableValues) throws Exception{
		Boolean dataProcessed = true;
	  	initiateCosVariables();
		
	  	//System.out.println(sTableName);

		switch (sTableName) {			
			case LiveDataConsts.OMS_TABLE_ORDER_HEADER:
				updateOrderHeaderDataInMap(lstTableColumns, lstTableValues);
				break;
				
			case LiveDataConsts.OMS_TABLE_ORDER_LINE:
				orderLinesList.add(new OrderLine(lstTableColumns, lstTableValues));				
				break;
				
				//for items no other mapping is eneded so we create input to cos data
			case LiveDataConsts.OMS_TABLE_ITEM:
				appendDataToCOSMap(new Item(lstTableColumns, lstTableValues).getItemJSON(), LiveDataConsts.COS_ITEM);
				break;
				
			case LiveDataConsts.OMS_TABLE_SHIP_NODE:
				shipNodeList.add(new Organization(lstTableColumns, lstTableValues));
				break;
				
			case LiveDataConsts.OMS_PERSON_INFO:
				updateOrderPersonInfoMap(lstTableColumns, lstTableValues);
				break;
			
			default:
				break;
		}
		
		return dataProcessed;
	}

   	public void updateOrderHeaderDataInMap(List<String> lstTableColumns, List<String> lstTableValues) throws Exception {		
   		int iTableValuesOffset = 0;
   		for (String sTableColumn : lstTableColumns){			
			if(LiveDataConsts.OMS_ORDER_HEADER_KEY.equals(sTableColumn)) {
				OrderHeader orderHeaderObj = new OrderHeader(lstTableColumns, lstTableValues);
				String orderHK = LiveDataUtils.removeUnwantedCharacters(
						lstTableValues.get(iTableValuesOffset), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
				
				orderHeaderMp.put(orderHK, orderHeaderObj);				
				appendDataToCOSMap(orderHeaderObj.getOrderJSON(), LiveDataConsts.COS_ORDER_HEADER);
				break;
			}
			iTableValuesOffset++;	
		}		
	}
   	
   	public void updateOrderPersonInfoMap(List<String> lstTableColumns, List<String> lstTableValues) throws Exception {		
   		int iTableValuesOffset = 0;
   		for (String sTableColumn : lstTableColumns){			
			if(LiveDataConsts.OMS_PERSON_INFO_KEY.equals(sTableColumn)) {
				PersonInfo personInfoObj = new PersonInfo(lstTableColumns, lstTableValues);
				
				String personInfoKey = LiveDataUtils.removeUnwantedCharacters(
						lstTableValues.get(iTableValuesOffset), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
				
				personInfoMp.put(personInfoKey, personInfoObj);				
				appendDataToCOSMap(personInfoObj.getPersonInfoJSON(), LiveDataConsts.COS_CONTACT);
				break;
			}
			iTableValuesOffset++;	
		}		
	}

   	public void createJsonFromLineData() throws Exception {   				
   		if(orderLinesList !=null) {
   			for(OrderLine orderLineObj: orderLinesList) {
   	   			//appending order header data
   	   			orderLineObj.updateLineJSONwithOrderData(orderHeaderMp.get(orderLineObj.getOrderHeaderKey()));
   				appendDataToCOSMap(orderLineObj.getLineRootJSON(), LiveDataConsts.COS_ORDER_LINE);   					
   	   			
   	   		}
   		}		
	}
   	

	private void createJsonFromShipNodeData() throws Exception {
		if(shipNodeList !=null) {
   			for(Organization orgObj: shipNodeList) {
   	   			//appending person info
   				PersonInfo pInfoObj = personInfoMp.get(orgObj.getOrgAddressKey());
   				if(pInfoObj !=null) {
   					orgObj.generateOrgJSON(pInfoObj);
   	   				appendDataToCOSMap(orgObj.getOrgJSON(), LiveDataConsts.COS_CORG);
   				}  					
   	   			
   	   		}
   		}	
		
	}

   	private void appendDataToCOSMap(JSONObject jsonData, String dataType) {
		StringBuilder cosInputBuilder = getCosInputBuilderFromMap(dataType);
		cosInputBuilder.append(jsonData.toString());
		cosInputMap.put(dataType, cosInputBuilder.toString());
	}
	
	private StringBuilder getCosInputBuilderFromMap(String dataType) {
		StringBuilder inputBuilder = null;
		if(cosInputMap.containsKey(dataType)) {			
			inputBuilder = new StringBuilder(cosInputMap.get(dataType));
			inputBuilder.append("\n");		
		}else {
			inputBuilder = new StringBuilder();

		}
			
		return inputBuilder;
	}
	
	public void uploadFileFromText(String fileName, String fileText) throws Exception {    
		String itemName = null;
		try {
			if(ktdClient.saveCOSDataToFile()) {
				itemName = ktdClient.getCosLocalFileLocation()
						.concat(fileName).concat("-") + System.currentTimeMillis() + ".txt"; 
				FileUtils.writeStringToFile(new File(itemName), fileText, Charset.forName("UTF-8"));
				logger.info("File pushed to cloud: " + itemName);
				
			}else {
				
				//creating a unique name
				itemName = "pw/pw_".concat(fileName).concat("-") + System.currentTimeMillis();
				byte[] arr = fileText.getBytes(StandardCharsets.UTF_8);
				InputStream newStream = new ByteArrayInputStream(arr);
				
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(arr.length);
				
				List<Tag> tagsList = new ArrayList<Tag>();
				tagsList.add(new Tag(LiveDataConsts.SCIS_TENANT_ID, SCIS_TENANT_ID));        
				ObjectTagging objTag = new ObjectTagging(tagsList);        
				
				PutObjectRequest req = new PutObjectRequest(COS_BUCKET_NAME, itemName, newStream, metadata);
				req.setTagging(objTag);
				cosClient.putObject(req);
				
				logger.info("File pushed to cloud: " + itemName);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
    }
    
	public AmazonS3 createClient(){
    	AWSCredentials credentials = new BasicIBMOAuthCredentials(COS_API_KEY_ID, COS_SERVICE_CRN);
        ClientConfiguration clientConfig = new ClientConfiguration()
             .withRequestTimeout(5000)
             .withTcpKeepAlive(true);

        return AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(new EndpointConfiguration(COS_ENDPOINT, COS_BUCKET_LOCATION))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfig)
                    .build();
    }
    
	private void initiateCosVariables() {	
		if(COS_ENDPOINT == null) COS_ENDPOINT = ktdClient.getCosEndPoint();
		if(COS_API_KEY_ID == null) COS_API_KEY_ID = ktdClient.getCosApiKey();
		if(COS_SERVICE_CRN == null) COS_SERVICE_CRN = ktdClient.getCosServiceCRN();
		if(COS_BUCKET_LOCATION == null) COS_BUCKET_LOCATION = ktdClient.getCosBucketLocation();
		if(COS_BUCKET_NAME == null) COS_BUCKET_NAME = ktdClient.getCosBucketName();
		if(SCIS_TENANT_ID == null) SCIS_TENANT_ID = ktdClient.getSCISTenantId();
		if(cosClient == null) cosClient = createClient();
		if(cosInputMap == null) cosInputMap = new HashMap<String, String>();
		if(orderHeaderMp == null) orderHeaderMp = new HashMap<String, OrderHeader>();
		if(orderLinesList == null) orderLinesList = new ArrayList<OrderLine>();
		if(shipNodeList == null) shipNodeList = new ArrayList<Organization>();
		if(personInfoMp == null) personInfoMp = new HashMap<String, PersonInfo>();
		
	}
	

}
