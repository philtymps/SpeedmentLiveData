package com.speedment.livedata.data.types;

import java.util.List;

import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class Organization {

	String orgID;
	String orgName;
	String orgAddressID;
	String orgType;
	
	public Organization(List<String> columnList, List<String> valueList) {
		initializeOrderData(columnList, valueList);
	}
	
	private void initializeOrderData(List<String> columnList, List<String> valueList) {
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
}
