package com.speedment.livedata.client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
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
import com.speedment.livedata.data.types.OrderHeader;
import com.speedment.livedata.data.types.OrderLine;
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
   	private HashMap<String, OrderLine> orderLineMp = null;
   	private LiveDataKafkaToDBClient ktdClient = null;
   	private LiveDataUtils ldUtils = null;
   	
   	private Logger logger = Logger.getLogger(LiveDataKafkaToCOSRunner.class);
   	
   	public LiveDataKafkaToCOSRunner(LiveDataKafkaToDBClient ktdInputObj) {
   		ktdClient = ktdInputObj;
   	   	initiateCosVariables();
   	}
   	
    public void publishCOSDataToCloud() throws Exception {		
    	createOrderJsonFromOrderData();
    	createJsonFromLineData();
    	if(cosInputMap !=null) {
    		cosInputMap.forEach((k,v) ->
	    		uploadFileFromText(k, v)
	    	);
    	}
    	cosInputMap = null;
    	orderHeaderMp = null;
    	orderLineMp = null;
	} 
   	
   	public boolean processCOSDataRecord(String sTableName, String sTableColumns, 
			List<String> lstTableColumns, List<String> lstTableValues) throws Exception{
		Boolean dataProcessed = true;
	  	initiateCosVariables();
		
	  	//TODO: need to add more tables to list
		switch (sTableName) {			
			case LiveDataConsts.OMS_TABLE_ORDER_HEADER:
				updateOrderDataInMap(lstTableColumns, lstTableValues, LiveDataConsts.OMS_TABLE_ORDER_HEADER);
				break;
				
			case LiveDataConsts.OMS_TABLE_ORDER_LINE:
				updateOrderDataInMap(lstTableColumns, lstTableValues, LiveDataConsts.OMS_TABLE_ORDER_LINE);				
				break;
			
			default:
				break;
		}
		
		return dataProcessed;
	}


   	/*
   	 * this method takes input for order and order line and populates the values to below maps
   	 * - orderHeaderMp 
	 *- orderLineMp
   	 * 
   	 */
   	public void updateOrderDataInMap(List<String> lstTableColumns, List<String> lstTableValues, String dataType) {		
   		int iTableValuesOffset = 0;
   		
   		if(LiveDataConsts.OMS_TABLE_ORDER_HEADER.equals(dataType)) {
			for (String sTableColumn : lstTableColumns){			
				if(LiveDataConsts.OMS_ORDER_HEADER_KEY.equals(sTableColumn)) {				
					orderHeaderMp.put(lstTableValues.get(iTableValuesOffset), 
							new OrderHeader(lstTableColumns, lstTableValues));
					break;
				}
				iTableValuesOffset++;	
			}
		}
		
   		if(LiveDataConsts.OMS_TABLE_ORDER_LINE.equals(dataType)) {
			for (String sTableColumn : lstTableColumns){			
				if(LiveDataConsts.OMS_ORDER_HEADER_KEY.equals(sTableColumn)) {				
					orderLineMp.put(lstTableValues.get(iTableValuesOffset), 
							new OrderLine(lstTableColumns, lstTableValues));
					break;
				}
				iTableValuesOffset++;	
			}
		}   				
	}

   	/*
   	 * This method is triggered aftera all data is downloaded from Kafka
   	 * 1. It reads values from orderHeaderMap
   	 * 2. Converts data into JSON and makes one entry per line
   	 * 3. Adds the JSON into cosInputMap 
   	 */
   	public void createOrderJsonFromOrderData() throws Exception{
		if(orderHeaderMp !=null) {
			try {
				for(Map.Entry<String, OrderHeader> entry : orderHeaderMp.entrySet()) {			    
				    OrderHeader orderHeaderObj = entry.getValue();
				
				    String orderNo = ldUtils.removeUnwantedCharacters(orderHeaderObj.getOrderNo());
				    String orderType = ldUtils.removeUnwantedCharacters(orderHeaderObj.getDocumentType());
				    
				    
					StringBuilder cosInput = getCosInputBuilderFromMap(LiveDataConsts.COS_ORDER_HEADER); 
					
					//creating the base json
					JSONObject orderRootObj = ldUtils.createRootJsonForCOS();
					JSONObject businessObject = orderRootObj
							.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
							.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
					
					appendOrderAttributesToJSONInput(businessObject, orderHeaderObj);

					businessObject.put(LiveDataConsts.SCIS_GLOBAL_IDENTIFIERS, 
							new JSONArray()
							.put(new JSONObject()
								.put(LiveDataConsts.SCIS_NAME, LiveDataConsts.SCIS_STERLING_SC_GLOBALID)
								.put(LiveDataConsts.SCIS_VALUE,ldUtils.getOrderNumber(orderNo,ldUtils.getOrderType(orderType))))
						);
					
					businessObject.put(LiveDataConsts.SCIS_ORDER_IDENTIFIER, orderNo);
					businessObject.put(LiveDataConsts.SCIS_ORDER_TYPE, ldUtils.getOrderType(orderType));
					
					logger.debug("appending cos data: " + orderRootObj.toString());
					cosInput.append(orderRootObj.toString());
					cosInputMap.put(LiveDataConsts.COS_ORDER_HEADER, cosInput.toString());		
				
				}
			} catch (Exception e) {			
				e.printStackTrace();
			} 	

		}
				
	}

   	/*
   	 * This method is triggered after all data is downloaded from Kafka
   	 * 1. It reads values from orderLineMap
   	 * 2. Converts data into JSON and makes one entry per line
   	 * 3. Adds the JSON into cosInputMap 
   	 */
   	public void createJsonFromLineData() throws Exception {

   		if(orderLineMp !=null) {
   			try {
   				
   				for(Entry<String, OrderLine> entry : orderLineMp.entrySet()) {			    
   				    
   					OrderLine orderLineObj = entry.getValue();
   					StringBuilder cosInput = getCosInputBuilderFromMap(LiveDataConsts.COS_ORDER_LINE); 
   					
   					//creating the base json
   					JSONObject orderRootObj = ldUtils.createRootJsonForCOS();
   					JSONObject businessObject = orderRootObj
   							.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
   							.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 

   					JSONObject orderLineJsonObj = new JSONObject();

   					appendLineAttributesToISONInput(orderLineJsonObj, orderLineObj);
   					
   					OrderHeader orderObj = orderHeaderMp.get(orderLineObj.getOrderHeaderKey());
   					
   					String orderNo = ldUtils.removeUnwantedCharacters(orderObj.getOrderNo());
   					String orderType = ldUtils.removeUnwantedCharacters(orderObj.getDocumentType());				
   					
   					businessObject.put(LiveDataConsts.SCIS_GLOBAL_IDENTIFIERS, 
   							new JSONArray()
   							.put(new JSONObject()
   								.put(LiveDataConsts.SCIS_NAME, LiveDataConsts.SCIS_STERLING_SC_GLOBALID)
   								.put(LiveDataConsts.SCIS_VALUE, ldUtils.getOrderNumber(orderNo, ldUtils.getOrderType(orderType))))
   						);
   					
   					businessObject.put(LiveDataConsts.SCIS_ORDER_IDENTIFIER, orderNo);
   					businessObject.put(LiveDataConsts.SCIS_ORDER_TYPE, ldUtils.getOrderType(orderType));
   					
   					businessObject.put(LiveDataConsts.SCIS_ORDER_LINES, new JSONArray().put(orderLineJsonObj));
   					
   					orderLineJsonObj.put(LiveDataConsts.SCIS_GLOBAL_IDENTIFIERS, 
   							new JSONArray()
   							.put(new JSONObject()
   								.put(LiveDataConsts.SCIS_NAME, LiveDataConsts.SCIS_STERLING_SC_GLOBALID)
   								.put(LiveDataConsts.SCIS_VALUE, ldUtils.getOrderNumber(orderNo, ldUtils.getOrderType(orderType))))
   						);
   					orderLineJsonObj.put(LiveDataConsts.SCIS_CREATED_DATE, orderObj.getOrderDate());
   					orderLineJsonObj.put(LiveDataConsts.SCIS_VALUE_CURRENCY, orderObj.getCurrency());
   					
   					logger.debug("appending cos data: " + orderRootObj.toString());
   					cosInput.append(orderRootObj.toString());
   					cosInputMap.put(LiveDataConsts.COS_ORDER_LINE, cosInput.toString());
   				}
   			} catch (Exception e) {
   				// TODO Auto-generated catch block
   				e.printStackTrace();
   			}
   		}		
	}
   	
	private void appendOrderAttributesToJSONInput(JSONObject businessObject, 
			OrderHeader orderHeaderObj) throws Exception {
	
		businessObject.put(LiveDataConsts.SCIS_BUYER, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderHeaderObj.getBillToID())));
		
		businessObject.put(LiveDataConsts.SCIS_CREATED_DATE, 
				ldUtils.removeUnwantedCharacters(orderHeaderObj.getOrderDate()));		
				
		businessObject.put(LiveDataConsts.SCIS_ORDER_VALUE_CURRENCY, 
				ldUtils.removeUnwantedCharacters(orderHeaderObj.getCurrency()));
			
		businessObject.put(LiveDataConsts.SCIS_PLANNED_DEL_DATE, 
				ldUtils.removeUnwantedCharacters(orderHeaderObj.getReqDeliveryDate()));
			
		businessObject.put(LiveDataConsts.SCIS_PLANNED_SHIP_DATE, 
				ldUtils.removeUnwantedCharacters(orderHeaderObj.getReqShipDate()));
				
		businessObject.put(LiveDataConsts.SCIS_SHIP_FROM_INSTR_LOCATION, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderHeaderObj.getSellerOrg())));
		
		businessObject.put(LiveDataConsts.SCIS_VENDOR, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderHeaderObj.getSellerOrg())));

		businessObject.put(LiveDataConsts.SCIS_SHIP_TO_LOCATION, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderHeaderObj.getShipToID())));
		
	}
	
   	private void appendLineAttributesToISONInput(JSONObject orderLineJsonObj, 
			OrderLine orderLineObj) throws Exception {
		
   		orderLineJsonObj.put(LiveDataConsts.SCIS_ORDER_LINE_NO, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getPrimeLineNO()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_REQ_DELIVERY_DATE, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getReqDeliveryDate()));

		orderLineJsonObj.put(LiveDataConsts.SCIS_REQ_SHIP_DATE, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getReqShipDate()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_PRODUCT, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderLineObj.getItemID())));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_PRODUCT_VALUE, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getUnitPrice()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_QUANTITY, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getOrderedQty()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_QUANTITY_UNITS, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getUom()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_PLANNED_DEL_DATE, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getEarliestDeliveryDate()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_PLANNED_SHIP_DATE, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getEarliestShipDate()));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_SHIP_FROM_INSTR_LOCATION, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderLineObj.getShipNode())));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_SHIP_TO_LOCATION, 
				ldUtils.createGlobalIdentifier(ldUtils.removeUnwantedCharacters(orderLineObj.getShipToID())));
				
		orderLineJsonObj.put(LiveDataConsts.SCIS_VALUE, 
				ldUtils.removeUnwantedCharacters(orderLineObj.getLineTotal()));
		
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
	
	public void uploadFileFromText(String fileName, String fileText) {    
    	
    	try {
			//creating a unique name
			String itemName = "pw/pw_".concat(fileName).concat("-") + System.currentTimeMillis();
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
		
		if(COS_ENDPOINT == null)
			COS_ENDPOINT = ktdClient.getCosEndPoint();
	   	
		if(COS_API_KEY_ID == null)
			COS_API_KEY_ID = ktdClient.getCosApiKey();
		
		if(COS_SERVICE_CRN == null)
			COS_SERVICE_CRN = ktdClient.getCosServiceCRN();
	   	
		if(COS_BUCKET_LOCATION == null)
			COS_BUCKET_LOCATION = ktdClient.getCosBucketLocation();
	   	
		if(COS_BUCKET_NAME == null)
			COS_BUCKET_NAME = ktdClient.getCosBucketName();
	   	
		if(SCIS_TENANT_ID == null)
			SCIS_TENANT_ID = ktdClient.getSCISTenantId();
	   	
		if(cosClient == null)
			cosClient = createClient();
	   	
		if(cosInputMap == null)
			cosInputMap = new HashMap<String, String>();
		
		if(orderHeaderMp == null)
			orderHeaderMp = new HashMap<String, OrderHeader>();
		
		if(orderLineMp == null)
			orderLineMp = new HashMap<String, OrderLine>();
		
		ldUtils = new LiveDataUtils();
	}
	

}
