package com.speedment.livedata.data.types;

import java.util.List;

import com.speedment.livedata.global.LiveDataConsts;

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
	
	public OrderHeader(List<String> columnList, List<String> valueList) {
		initializeOrderData(columnList, valueList);
	}
	
	private void initializeOrderData(List<String> columnList, List<String> valueList) {
		int index = 0;
		for (String sTableColumn : columnList){			
			
			switch (sTableColumn) {
			case LiveDataConsts.OMS_ORDER_HEADER_KEY:				
				orderHK = valueList.get(index);
				break;
			
			case LiveDataConsts.OMS_ORDER_NO:				
				orderNo = valueList.get(index);
				break;
				
			case LiveDataConsts.OMS_DOC_TYPE:				
				documentType = valueList.get(index);
				break;	
			
			case LiveDataConsts.OMS_CURRENCY:				
				currency = valueList.get(index);
				break;
			
			case LiveDataConsts.OMS_BILLTO_ID:				
				billToID = valueList.get(index);
				break;
			
			case LiveDataConsts.OMS_REQ_DELIVERY_DATE:				
				reqDeliveryDate = valueList.get(index);
				break;
			
			case LiveDataConsts.OMS_REQ_SHIP_DATE:				
				reqShipDate = valueList.get(index);
				break;
			case LiveDataConsts.OMS_SELLEER_ORG:				
				sellerOrg = valueList.get(index);
				break;
			case LiveDataConsts.OMS_SHIP_ID:				
				shipToID = valueList.get(index);
				break;			
			
			default:
				break;
			}			
			index++;	
		}	
		
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

	
}
