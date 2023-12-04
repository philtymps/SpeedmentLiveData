package com.speedment.livedata.data.types;

import java.util.List;

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
	
	public Item(List<String> columnList, List<String> valueList) {
		initializeOrderData(columnList, valueList);
	}
	
	private void initializeOrderData(List<String> columnList, List<String> valueList) {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(valueList.get(index));
			
			switch (sTableColumn) {
				case LiveDataConsts.OMS_ITEM_ID:				
					itemID = value;
					break;
				case LiveDataConsts.OMS_DESCRIPTION:				
					description = value;
					break;
				
				case LiveDataConsts.OMS_SHORT_DESC:				
					shortDescription = value;
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
}
