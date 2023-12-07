package com.speedment.livedata.data.types;

import java.util.List;
import org.json.JSONObject;

import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class OrderHeader {
	private String orderHK;
	private String orderNo;
	private String documentType;
	private String orderDate;
	private String currency;
	private String billToID;
	private String reqDeliveryDate;
	private String reqShipDate;
	private String sellerOrg;
	private String shipToID;
	private String totalAmount;
	private JSONObject orderJSON;

	public OrderHeader(List<String> columnList, List<String> valueList) throws Exception {
		initializeOrderData(columnList, valueList);
		generateJsonData();
	}

	private void initializeOrderData(List<String> columnList, List<String> valueList) {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(
					valueList.get(index), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
			
			switch (sTableColumn) {
			case LiveDataConsts.OMS_ORDER_HEADER_KEY:				
				orderHK = value;
				break;
			
			case LiveDataConsts.OMS_ORDER_NO:				
				orderNo = value;
				break;
				
			case LiveDataConsts.OMS_DOC_TYPE:				
				documentType = value;
				break;	
			
			case LiveDataConsts.OMS_CURRENCY:				
				currency = value;
				break;
			
			case LiveDataConsts.OMS_BILLTO_ID:				
				billToID = value;
				break;
			
			case LiveDataConsts.OMS_REQ_DELIVERY_DATE:				
				reqDeliveryDate = LiveDataUtils.formatDate(value);
				break;
			
			case LiveDataConsts.OMS_REQ_SHIP_DATE:				
				reqShipDate = LiveDataUtils.formatDate(value);
				break;
			
			case LiveDataConsts.OMS_SELLEER_ORG:				
				sellerOrg = value;
				break;
			
			case LiveDataConsts.OMS_SHIP_ID:				
				shipToID = value;
				break;
			
			case LiveDataConsts.OMS_ORDER_DATE:				
				orderDate = LiveDataUtils.formatDate(value);
				break;
				
			case LiveDataConsts.OMS_TOTAL_AMT:				
				totalAmount = value;
				break;
				
			default:
				break;
			}			
			index++;	
		}	
		
	}
	
	private void generateJsonData() throws Exception {
		orderJSON = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.SCIS_TYPE_ORDER);
		JSONObject businessObject = orderJSON
				.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
				.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 

		businessObject.put(LiveDataConsts.SCIS_CREATED_DATE, getOrderDate());
		businessObject.put(LiveDataConsts.SCIS_ORDER_VALUE_CURRENCY, getCurrency());
		businessObject.put(LiveDataConsts.SCIS_PLANNED_DEL_DATE, getReqDeliveryDate());
		businessObject.put(LiveDataConsts.SCIS_PLANNED_SHIP_DATE, getReqShipDate());
		businessObject.put(LiveDataConsts.SCIS_REQ_DELIVERY_DATE, getReqDeliveryDate());
		businessObject.put(LiveDataConsts.SCIS_REQ_SHIP_DATE, getReqShipDate());
		businessObject.put(LiveDataConsts.SCIS_TOTAL_VALUE, getTotalAmount());

		businessObject.put(LiveDataConsts.SCIS_BUYER, 
				LiveDataUtils.createGlobalIdentifier(getBillToID()));
				
		businessObject.put(LiveDataConsts.SCIS_SHIP_FROM_INSTR_LOCATION, 
				LiveDataUtils.createGlobalIdentifier(getSellerOrg()));

		businessObject.put(LiveDataConsts.SCIS_VENDOR, 
				LiveDataUtils.createGlobalIdentifier(getSellerOrg()));

		businessObject.put(LiveDataConsts.SCIS_SHIP_TO_LOCATION, 
				LiveDataUtils.createGlobalIdentifier(getShipToID()));

		String orderNo = LiveDataUtils.removeUnwantedCharacters(
				getOrderNo(),LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
		
		String orderType = LiveDataUtils.removeUnwantedCharacters(
				getDocumentType(), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
		
		LiveDataUtils.createRootGlobalIdentifier(businessObject, 
				LiveDataUtils.getOrderNumber(orderNo, orderType));

		businessObject.put(LiveDataConsts.SCIS_ORDER_IDENTIFIER, orderNo);
		businessObject.put(LiveDataConsts.SCIS_ORDER_TYPE, LiveDataUtils.getOrderType(orderType));
	}
	
	public String getOrderHK() {
		return orderHK;
	}

	public void setOrderHK(String orderHkey) {
		this.orderHK = orderHkey;
	}
	
	public String getOrderNo() {
		return orderNo;
	}

	public void setOrderNo(String orderNo) {
		this.orderNo = orderNo;
	}

	public String getDocumentType() {
		return documentType;
	}

	public void setDocumentType(String documentType) {
		this.documentType = documentType;
	}

	public String getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(String orderDate) {
		this.orderDate = orderDate;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public String getBillToID() {
		return billToID;
	}

	public void setBillToID(String billToID) {
		this.billToID = billToID;
	}

	public String getReqDeliveryDate() {
		return reqDeliveryDate;
	}

	public void setReqDeliveryDate(String reqDeliveryDate) {
		this.reqDeliveryDate = reqDeliveryDate;
	}

	public String getReqShipDate() {
		return reqShipDate;
	}

	public void setReqShipDate(String reqShipDate) {
		this.reqShipDate = reqShipDate;
	}

	public String getSellerOrg() {
		return sellerOrg;
	}

	public void setSellerOrg(String sellerOrg) {
		this.sellerOrg = sellerOrg;
	}

	public String getShipToID() {
		return shipToID;
	}

	public void setShipToID(String shipToID) {
		this.shipToID = shipToID;
	}

	
	public String getTotalAmount() {
		return totalAmount;
	}

	public void setTotalAmount(String totalAmount) {
		this.totalAmount = totalAmount;
	}

	public JSONObject getOrderJSON() {
		return orderJSON;
	}
	
}
