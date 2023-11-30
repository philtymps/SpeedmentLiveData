package com.speedment.livedata.global;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class LiveDataUtils {
	
	public String getOrderNumber(String orderNo, String orderType) {
		if(orderType != null) {
			return orderNo.concat(".".concat(orderType));
		}
		return orderNo;
	}
	
	public JSONObject createGlobalIdentifier(String sTableValue) throws JSONException {
		return new JSONObject()
		.put(LiveDataConsts.SCIS_GLOBAL_IDENTIFIERS, 
				new JSONArray()
				.put(new JSONObject()
					.put(LiveDataConsts.SCIS_NAME, LiveDataConsts.SCIS_STERLING_SC_GLOBALID)
					.put(LiveDataConsts.SCIS_VALUE, sTableValue))
		);
	}

	public String getOrderType(String sTableValue) {

		if(LiveDataConsts.OMS_PURCHASE_ORDER.equals(sTableValue))
			return LiveDataConsts.SCIS_INBOUND;
		
		if(LiveDataConsts.OMS_TRANSFER_ORDER.equals(sTableValue))
			return LiveDataConsts.SCIS_TRANSFER;
		
		return LiveDataConsts.SCIS_OUTBOUND;
	}

	/* this method creates below root json object that can be used for order related data
	{
	    "eventCode": "objectUpsertEvent",
	    "eventDetails": {
	        "businessObject": {
	            "globalIdentifiers": [
	                {
	                    "name": "sterlingSupplyChain.globalId",
	                    "value": "Customer-test"
	                }
	            ],
	            "type": "Order"
	        }
	    }
	}
	*/
	public JSONObject createRootJsonForCOS() throws JSONException {		
		return new JSONObject()		
			.put(LiveDataConsts.SCIS_EVENT_CODE,LiveDataConsts.SCIS_OBJECT_UPSERT_EVENT)
			.put(LiveDataConsts.SCIS_EVENT_DETAILS, 
				new JSONObject().put(LiveDataConsts.SCIS_BUSINESS_OBJECT,
					new JSONObject()						
						.put(LiveDataConsts.SCIS_TYPE, LiveDataConsts.SCIS_TYPE_ORDER)
					)
			);
		
	}
	
	public String removeUnwantedCharacters(String attribute) {
		if(attribute != null)
			return attribute.replaceAll("'","");
		return attribute;
	}
}
