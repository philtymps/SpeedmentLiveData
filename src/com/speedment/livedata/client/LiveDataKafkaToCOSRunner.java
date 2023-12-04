package com.speedment.livedata.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
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
import com.speedment.livedata.data.types.Item;
import com.speedment.livedata.data.types.OrderHeader;
import com.speedment.livedata.data.types.OrderLine;
import com.speedment.livedata.data.types.Organization;
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
   	private List<Item> itemsList = null;
   	private List<Organization> orgList = null;
   	
   	private Logger logger = Logger.getLogger(LiveDataKafkaToCOSRunner.class);
   	
   	public LiveDataKafkaToCOSRunner(LiveDataKafkaToDBClient ktdInputObj) {
   		ktdClient = ktdInputObj;
   	   	initiateCosVariables();
   	}
   	
    public void publishCOSDataToCloud() throws Exception {		
    	createOrderJsonFromOrderData();
    	createJsonFromLineData();
    	createJsonFromItemData();
    	createJsonFromOrgData();
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
	} 

	public boolean processCOSDataRecord(String sTableName, String sTableColumns, 
			List<String> lstTableColumns, List<String> lstTableValues) throws Exception{
		Boolean dataProcessed = true;
	  	initiateCosVariables();
		
	  	//TODO: need to add more tables to list
		switch (sTableName) {			
			case LiveDataConsts.OMS_TABLE_ORDER_HEADER:
				updateOrderHeaderDataInMap(lstTableColumns, lstTableValues);
				break;
				
			case LiveDataConsts.OMS_TABLE_ORDER_LINE:
				orderLinesList.add(new OrderLine(lstTableColumns, lstTableValues));				
				break;
				
			case LiveDataConsts.OMS_TABLE_ITEM:
				itemsList.add(new Item(lstTableColumns, lstTableValues));				
				break;
				
			case LiveDataConsts.OMS_TABLE_SHIP_NODE:
				orgList.add(new Organization(lstTableColumns, lstTableValues));				
				break;
			
			default:
				break;
		}
		
		return dataProcessed;
	}

   	public void updateOrderHeaderDataInMap(List<String> lstTableColumns, List<String> lstTableValues) {		
   		int iTableValuesOffset = 0;
   		for (String sTableColumn : lstTableColumns){			
			if(LiveDataConsts.OMS_ORDER_HEADER_KEY.equals(sTableColumn)) {				
				orderHeaderMp.put(LiveDataUtils.removeUnwantedCharacters(lstTableValues.get(iTableValuesOffset)), 
						new OrderHeader(lstTableColumns, lstTableValues));
				break;
			}
			iTableValuesOffset++;	
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
					
					//creating the base json
					JSONObject orderRootObj = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.SCIS_TYPE_ORDER);
					JSONObject businessObject = orderRootObj
							.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
							.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
					
					businessObject.put(LiveDataConsts.SCIS_CREATED_DATE, orderHeaderObj.getOrderDate());
					businessObject.put(LiveDataConsts.SCIS_ORDER_VALUE_CURRENCY, orderHeaderObj.getCurrency());
					businessObject.put(LiveDataConsts.SCIS_PLANNED_DEL_DATE, orderHeaderObj.getReqDeliveryDate());
					businessObject.put(LiveDataConsts.SCIS_PLANNED_SHIP_DATE, orderHeaderObj.getReqShipDate());
					businessObject.put(LiveDataConsts.SCIS_REQ_DELIVERY_DATE, orderHeaderObj.getReqDeliveryDate());
					businessObject.put(LiveDataConsts.SCIS_REQ_SHIP_DATE, orderHeaderObj.getReqShipDate());
					businessObject.put(LiveDataConsts.SCIS_TOTAL_VALUE, orderHeaderObj.getTotalAmount());
					
					businessObject.put(LiveDataConsts.SCIS_BUYER, 
							LiveDataUtils.createGlobalIdentifier(orderHeaderObj.getBillToID()));
							
					businessObject.put(LiveDataConsts.SCIS_SHIP_FROM_INSTR_LOCATION, 
							LiveDataUtils.createGlobalIdentifier(orderHeaderObj.getSellerOrg()));
					
					businessObject.put(LiveDataConsts.SCIS_VENDOR, 
							LiveDataUtils.createGlobalIdentifier(orderHeaderObj.getSellerOrg()));

					businessObject.put(LiveDataConsts.SCIS_SHIP_TO_LOCATION, 
							LiveDataUtils.createGlobalIdentifier(orderHeaderObj.getShipToID()));

					String orderNo = LiveDataUtils.removeUnwantedCharacters(orderHeaderObj.getOrderNo());
					String orderType = LiveDataUtils.removeUnwantedCharacters(orderHeaderObj.getDocumentType());
					LiveDataUtils.createRootGlobalIdentifier(businessObject, 
							LiveDataUtils.getOrderNumber(orderNo, orderType));
					
					businessObject.put(LiveDataConsts.SCIS_ORDER_IDENTIFIER, orderNo);
					businessObject.put(LiveDataConsts.SCIS_ORDER_TYPE, LiveDataUtils.getOrderType(orderType));
					
					logger.debug("appending cos data: " + orderRootObj.toString());
										
					appendDataToCOSMap(orderRootObj, LiveDataConsts.COS_ORDER_HEADER); 
				
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

   		if(orderLinesList !=null) {
   			try {
   				
   				for(OrderLine orderLineObj: orderLinesList) {			    
   				    
   					//creating the base json and related data
   					JSONObject orderRootObj = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.SCIS_TYPE_ORDER);
   					JSONObject businessObject = orderRootObj
   							.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
   							.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 

   					JSONObject orderLineJsonObj = new JSONObject();
   					businessObject.put(LiveDataConsts.SCIS_ORDER_LINES, new JSONArray().put(orderLineJsonObj));
   					
   					//appending line data
   					orderLineJsonObj.put(LiveDataConsts.SCIS_ORDER_LINE_NO, orderLineObj.getPrimeLineNO());				
   					orderLineJsonObj.put(LiveDataConsts.SCIS_REQ_DELIVERY_DATE, orderLineObj.getReqDeliveryDate());
   					orderLineJsonObj.put(LiveDataConsts.SCIS_REQ_SHIP_DATE, orderLineObj.getReqShipDate());
   					orderLineJsonObj.put(LiveDataConsts.SCIS_PRODUCT_VALUE,orderLineObj.getUnitPrice());				
   					orderLineJsonObj.put(LiveDataConsts.SCIS_QUANTITY, orderLineObj.getOrderedQty());				
   					orderLineJsonObj.put(LiveDataConsts.SCIS_QUANTITY_UNITS, orderLineObj.getUom());				
   					orderLineJsonObj.put(LiveDataConsts.SCIS_PLANNED_DEL_DATE, orderLineObj.getEarliestDeliveryDate());				
   					orderLineJsonObj.put(LiveDataConsts.SCIS_PLANNED_SHIP_DATE, orderLineObj.getEarliestShipDate());
   					orderLineJsonObj.put(LiveDataConsts.SCIS_VALUE, orderLineObj.getLineTotal());
   					
   					orderLineJsonObj.put(LiveDataConsts.SCIS_PRODUCT, 
   							LiveDataUtils.createGlobalIdentifier(orderLineObj.getItemID()));
   					
   					orderLineJsonObj.put(LiveDataConsts.SCIS_SHIP_FROM_INSTR_LOCATION, 
   							LiveDataUtils.createGlobalIdentifier(orderLineObj.getShipNode()));
   							
   					orderLineJsonObj.put(LiveDataConsts.SCIS_SHIP_TO_LOCATION, 
   							LiveDataUtils.createGlobalIdentifier(orderLineObj.getShipToID()));
   					
   					//appending order header data
   					OrderHeader orderObj = orderHeaderMp.get(orderLineObj.getOrderHeaderKey());
   					
   					if(orderObj !=null) {	   					
	   					//attaching order related data
	   					String orderNo = "";
	   					String orderType = "";
   						orderNo = orderObj.getOrderNo();
   	   					orderType = orderObj.getDocumentType();
   	   					
   	   					businessObject.put(LiveDataConsts.SCIS_ORDER_TYPE, LiveDataUtils.getOrderType(orderType));
   	   					businessObject.put(LiveDataConsts.SCIS_ORDER_IDENTIFIER, orderNo);
   	   					LiveDataUtils.createRootGlobalIdentifier(businessObject, 
   	   							LiveDataUtils.getOrderNumber(orderNo, orderType));
   	   					
   	   					LiveDataUtils.createRootGlobalIdentifier(businessObject, 
   	   							LiveDataUtils.getOrderNumber(orderNo, orderType));
   	   					
   	   					//line related order data
   		   				orderLineJsonObj.put(LiveDataConsts.SCIS_CREATED_DATE, orderObj.getOrderDate());
   		   				orderLineJsonObj.put(LiveDataConsts.SCIS_VALUE_CURRENCY, orderObj.getCurrency());
   	   					
   					}
   					
   					logger.debug("appending cos data: " + orderRootObj.toString());   					
   					appendDataToCOSMap(orderRootObj, LiveDataConsts.COS_ORDER_LINE);   					
   				}
   			} catch (Exception e) {
   				// TODO Auto-generated catch block
   				e.printStackTrace();
   			}
   		}		
	}
   	
   	private void createJsonFromItemData() throws Exception{
   		if(itemsList !=null) {
   			try {
   				
   				for(Item itemObj: itemsList) {	   					
   					//creating the base json and related data
   					JSONObject orderRootObj = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.COS_PRODUCT_LWR);
   					JSONObject businessObject = orderRootObj
   							.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
   							.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
   					
   					businessObject.put(LiveDataConsts.SCIS_BRAND, 
   							LiveDataUtils.createGlobalIdentifier(itemObj.getOrgCode()));
   					
   					businessObject.put(LiveDataConsts.SCIS_CATEGORY, 
   							LiveDataUtils.createGlobalIdentifier(itemObj.getDefaultProdClass()));
   					
   					businessObject.put(LiveDataConsts.SCIS_DEFAULT_QTY_UNIT, itemObj.getUom());
   					businessObject.put(LiveDataConsts.SCIS_ITEM_DESC, itemObj.getDescription());
   					businessObject.put(LiveDataConsts.SCIS_FAMILY, 
   							LiveDataUtils.createGlobalIdentifier(itemObj.getDefaultProdClass()));
   					
   					LiveDataUtils.createRootGlobalIdentifier(businessObject, itemObj.getItemID());
   					
   					businessObject.put(LiveDataConsts.SCIS_LINE, 
   							LiveDataUtils.createGlobalIdentifier(itemObj.getDefaultProdClass()));
   					
   					businessObject.put(LiveDataConsts.SCIS_NAME, itemObj.getShortDescription());
   					businessObject.put(LiveDataConsts.SCIS_PART_NO, itemObj.getItemID());
   					businessObject.put(LiveDataConsts.SCIS_PLANNER_CODE, itemObj.getDefaultProdClass());
   					businessObject.put(LiveDataConsts.SCIS_PROD_TYPE, LiveDataConsts.COS_PRODUCT);
   					businessObject.put(LiveDataConsts.SCIS_SOURCE_LINK, 
   							itemObj.getImageLocation().concat("/").concat(itemObj.getImageID()));
   					businessObject.put(LiveDataConsts.SCIS_STATUS, LiveDataConsts.COS_ACTIVE);
   					businessObject.put(LiveDataConsts.SCIS_VALUE, "0.00");
   					businessObject.put(LiveDataConsts.SCIS_VALUE_CURRENCY, "USD");
   					
   					logger.debug("appending cos data: " + orderRootObj.toString());   					
   					appendDataToCOSMap(orderRootObj, LiveDataConsts.COS_ITEM); 
   				}
   			} catch (Exception e) {
   				// TODO Auto-generated catch block
   				e.printStackTrace();
   			}
   		}
   	}
   	
   	private void createJsonFromOrgData() {
   		if(orgList !=null) {
   			try {
   				
   				for(Organization orgObj: orgList) {	   					
   					//creating the base json and related data
   					JSONObject orderRootObj = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.COS_ORGANIZATION);
   					JSONObject businessObject = orderRootObj
   							.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
   							.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
   					
   					LiveDataUtils.createRootGlobalIdentifier(businessObject, orgObj.getOrgID());
   					
   					businessObject.put(LiveDataConsts.SCIS_LOCATION, 
   							LiveDataUtils.createGlobalIdentifier(orgObj.getOrgAddressID()));
   					
   					businessObject.put(LiveDataConsts.SCIS_NAME, orgObj.getOrgName());
   					businessObject.put(LiveDataConsts.SCIS_ORG_IDENTIFIER, orgObj.getOrgID());
   					businessObject.put(LiveDataConsts.SCIS_ORG_TYPE, orgObj.getOrgType());
   					
   					logger.debug("appending cos data: " + orderRootObj.toString());   					
   					appendDataToCOSMap(orderRootObj, LiveDataConsts.COS_CORG); 
   				}	
   			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
   		}
	}
   	
   	private void appendDataToCOSMap(JSONObject orderRootObj, String dataType) {
		StringBuilder cosInputBuilder = getCosInputBuilderFromMap(dataType);
		cosInputBuilder.append(orderRootObj.toString());
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
    	
		/*
		//saving file to local machine for testing
			String itemName2 = "C:\\cos\\tmp\\".concat(fileName).concat("-") + System.currentTimeMillis() + ".txt"; 
			FileUtils.writeStringToFile(new File(itemName2), fileText, Charset.forName("UTF-8"));
			logger.info("File pushed to cloud: " + itemName2);
		*/
		
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
		if(itemsList == null) itemsList = new ArrayList<Item>();
		if(orgList == null) orgList = new ArrayList<Organization>();
		
	}
	

}
