package com.speedment.livedata.data.types;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class Item {

	String itemID;
	String description;
	String shortDescription;
	String imageLocation;
	String imageID;
	String defaultProdClass;	
	String orgCode;
	String uom;
	JSONObject itemJSON;

	public Item(List<String> columnList, List<String> valueList) throws Exception {
		initializeOrderData(columnList, valueList);
		generateItemJSON();
	}

	private void initializeOrderData(List<String> columnList, List<String> valueList) throws Exception {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(
					valueList.get(index), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
			
			switch (sTableColumn) {
				case LiveDataConsts.OMS_ITEM_ID:				
					itemID = value;
					break;
				case LiveDataConsts.OMS_DESCRIPTION:				
					description = URLDecoder.decode(value, "UTF-8");
					break;
				
				case LiveDataConsts.OMS_SHORT_DESC:				
					shortDescription = URLDecoder.decode(value, "UTF-8");
					break;
					
				case LiveDataConsts.OMS_IMAGE_LOCATION:				
					imageLocation = value;
					break;
					
				case LiveDataConsts.OMS_IMAGE_ID:				
					imageID = value;
					break;	
				
				case LiveDataConsts.OMS_DEFAULT_PROD_CLASS:				
					defaultProdClass = value;
					break;
				
				case LiveDataConsts.OMS_ORGANIZATION_CODE:				
					orgCode = value;
					break;
					
				case LiveDataConsts.OMS_UOM:				
					uom = value;
					break;
					
				default:
					break;
				}			
			index++;	
		}	
		
	}
	
	
	private void generateItemJSON() throws Exception {
		
		itemJSON = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.COS_PRODUCT_LWR);
		JSONObject businessObject = itemJSON
					.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
					.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
			
		businessObject.put(LiveDataConsts.SCIS_BRAND, 
					LiveDataUtils.createGlobalIdentifier(getOrgCode()));
			
		businessObject.put(LiveDataConsts.SCIS_CATEGORY, 
					LiveDataUtils.createGlobalIdentifier(getDefaultProdClass()));
			
		businessObject.put(LiveDataConsts.SCIS_DEFAULT_QTY_UNIT, getUom());
		businessObject.put(LiveDataConsts.SCIS_ITEM_DESC, getDescription());
		businessObject.put(LiveDataConsts.SCIS_FAMILY, 
					LiveDataUtils.createGlobalIdentifier(getDefaultProdClass()));
			
		LiveDataUtils.createRootGlobalIdentifier(businessObject, getItemID());
			
		businessObject.put(LiveDataConsts.SCIS_LINE, 
					LiveDataUtils.createGlobalIdentifier(getDefaultProdClass()));
			
		businessObject.put(LiveDataConsts.SCIS_NAME, getShortDescription());
		businessObject.put(LiveDataConsts.SCIS_PART_NO, getItemID());
		businessObject.put(LiveDataConsts.SCIS_PLANNER_CODE, getDefaultProdClass());
		businessObject.put(LiveDataConsts.SCIS_PROD_TYPE, LiveDataConsts.COS_PRODUCT);
		businessObject.put(LiveDataConsts.SCIS_SOURCE_LINK, 
					getImageLocation().concat("/").concat(getImageID()));
		businessObject.put(LiveDataConsts.SCIS_STATUS, LiveDataConsts.COS_ACTIVE);
		businessObject.put(LiveDataConsts.SCIS_VALUE, "0.00");
		businessObject.put(LiveDataConsts.SCIS_VALUE_CURRENCY, "USD");
		
	}

	
	public String getOrgCode() {
		return orgCode;
	}
	public void setOrgCode(String orgCode) {
		this.orgCode = orgCode;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getShortDescription() {
		return shortDescription;
	}
	public void setShortDescription(String shortDescription) {
		this.shortDescription = shortDescription;
	}
	public String getImageLocation() {
		return imageLocation;
	}
	public void setImageLocation(String imageLocation) {
		this.imageLocation = imageLocation;
	}
	public String getImageID() {
		return imageID;
	}
	public void setImageID(String imgageID) {
		this.imageID = imgageID;
	}
	public String getDefaultProdClass() {
		return defaultProdClass;
	}
	public void setDefaultProdClass(String defaultProdClass) {
		this.defaultProdClass = defaultProdClass;
	}
	
	public String getItemID() {
		return itemID;
	}

	public void setItemID(String itemID) {
		this.itemID = itemID;
	}
	
	public String getUom() {
		return uom;
	}

	public void setUom(String uom) {
		this.uom = uom;
	}
	
	public JSONObject getItemJSON() {
		return itemJSON;
	}

	public void setItemJSON(JSONObject itesmJSON) {
		this.itemJSON = itesmJSON;
	}
}
