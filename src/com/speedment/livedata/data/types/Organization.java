package com.speedment.livedata.data.types;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class Organization {

	String orgID;
	String orgName;
	String orgAddressID;
	String orgType;
	JSONObject orgJSON;


	public Organization(List<String> columnList, List<String> valueList) throws Exception {
		initializeOrgData(columnList, valueList);
		generateOrgJSON();
	}

	private void initializeOrgData(List<String> columnList, List<String> valueList) {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(valueList.get(index));
			
			switch (sTableColumn) {
				case LiveDataConsts.OMS_SHIP_NODE_KEY:				
					orgID = value;
					break;
				case LiveDataConsts.OMS_DESCRIPTION:				
					orgName = value;
					break;
				
				case LiveDataConsts.OMS_SHIP_ADDR_KEY:				
					orgAddressID = value;
					break;
					
				default:
					break;
				}			
			index++;	
		}	
		
	}
	
	
	private void generateOrgJSON() throws Exception {
		
		orgJSON = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.COS_ORGANIZATION);
		JSONObject businessObject = orgJSON
					.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
					.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
			
		LiveDataUtils.createRootGlobalIdentifier(businessObject, getOrgID());
			
		businessObject.put(LiveDataConsts.SCIS_LOCATION, 
					LiveDataUtils.createGlobalIdentifier(getOrgAddressID()));
			
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
}
