package com.speedment.livedata.data.types;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.ibm.cloud.objectstorage.util.StringUtils;
import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class Organization {

	String orgID;
	String orgName;
	String orgAddressID;
	String orgAddressKey;
	String orgType;
	JSONObject orgJSON;


	public Organization(List<String> columnList, List<String> valueList) throws Exception {
		initializeOrgData(columnList, valueList);
	}
	
	private void initializeOrgData(List<String> columnList, List<String> valueList) throws Exception {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(
					valueList.get(index), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
			
			switch (sTableColumn) {
				case LiveDataConsts.OMS_ORGANIZATION_CODE:				
					orgID = value;
					break;
				case LiveDataConsts.OMS_ORGANIZATION_NAME:				
					orgName = URLDecoder.decode(value, "UTF-8");
					break;
				
				case LiveDataConsts.OMS_CORPORATE_ADDRESS_KEY:				
					orgAddressKey = value;
					break;
					
				default:
					break;
				}			
			index++;	
		}	
		
	}
	
	
	public void generateOrgJSON(PersonInfo personInfo) throws Exception {
		
		String LocationID = 
				StringUtils.isNullOrEmpty(personInfo.getPersonID())? personInfo.getFirstName(): personInfo.getPersonID(); 
						
		orgJSON = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.COS_ORGANIZATION);
		JSONObject businessObject = orgJSON
					.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
					.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
			
		LiveDataUtils.createRootGlobalIdentifier(businessObject, getOrgID());
			
		businessObject.put(LiveDataConsts.SCIS_LOCATION_LWER, 
					LiveDataUtils.createGlobalIdentifier(StringUtils.isNullOrEmpty(LocationID)? orgID: LocationID));
			
		businessObject.put(LiveDataConsts.SCIS_NAME, getOrgName());
		businessObject.put(LiveDataConsts.SCIS_ORG_IDENTIFIER, getOrgID());
		businessObject.put(LiveDataConsts.SCIS_ORG_TYPE, getOrgType());
		
	}
	
	public String getOrgID() {
		return orgID;
	}
	public void setOrgID(String orgID) {
		this.orgID = orgID;
	}
	public String getOrgName() {
		return orgName;
	}
	public void setOrgName(String orgName) {
		this.orgName = orgName;
	}
	public String getOrgAddressID() {
		return orgAddressID;
	}
	public void setOrgAddressID(String orgAddressID) {
		this.orgAddressID = orgAddressID;
	}
	public String getOrgType() {
		return orgType;
	}
	public void setOrgType(String orgType) {
		this.orgType = orgType;
	}
	
	public JSONObject getOrgJSON() {
		return orgJSON;
	}

	public void setOrgJSON(JSONObject orgJSON) {
		this.orgJSON = orgJSON;
	}
	

	public String getOrgAddressKey() {
		return orgAddressKey;
	}

	public void setOrgAddressKey(String orgAddressKey) {
		this.orgAddressKey = orgAddressKey;
	}

}
