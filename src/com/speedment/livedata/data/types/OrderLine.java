package com.speedment.livedata.data.types;

import java.util.List;

import com.speedment.livedata.global.LiveDataConsts;


public class OrderLine {

	private String orderLineKey;
	private String orderHeaderKey;
	private String primeLineNO;
	private String reqShipDate;
	private String reqDeliveryDate;
	private String itemID;
	private String unitPrice;
	private String orderedQty;
	private String uom;
	private String earliestDeliveryDate;
	private String earliestShipDate;
	private String shipNode;
	private String shipToID;
	private String lineTotal;

	
	public OrderLine(List<String> columnList, List<String> valueList) {
		initializeOrderLineData(columnList, valueList);
	}
	
	private void initializeOrderLineData(List<String> columnList, List<String> valueList) {
		int index = 0;
		for (String sTableColumn : columnList){			
			
			switch (sTableColumn) {
				
				case LiveDataConsts.OMS_ORDER_LINE_KEY:				
					orderLineKey = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_ORDER_HEADER_KEY:				
					orderHeaderKey = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_PRIME_LINE_NO:				
					primeLineNO = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_REQ_DELIVERY_DATE:				
					reqDeliveryDate = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_REQ_SHIP_DATE:				
					reqShipDate = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_UNIT_PRICE:				
					unitPrice = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_ITEM_ID:				
					itemID = valueList.get(index);
					break;
				
				case LiveDataConsts.OMS_ORDERDED_QTY:				
					orderedQty = valueList.get(index);
					break;
					
				case LiveDataConsts.OMS_UOM:				
					uom = valueList.get(index);
					break;
					
				case LiveDataConsts.OMS_EARLIEST_DELIVERY_DATE:				
					earliestDeliveryDate = valueList.get(index);
					break;
					
				case LiveDataConsts.OMS_EARLIEST_SHIP_DATE:				
					earliestShipDate = valueList.get(index);
					break;
					
				case LiveDataConsts.OMS_SHIP_ID:				
					shipToID = valueList.get(index);
					break;
					
				case LiveDataConsts.OMS_SHIP_NODE:				
					shipNode = valueList.get(index);
					break;	
					
				case LiveDataConsts.OMS_LINE_TOTAL:				
					lineTotal = valueList.get(index);
					break;	
				
				default:
					break;
			}			
			index++;	
		}
	}
	
	public String getPrimeLineNO() {
		return primeLineNO;
	}

	public void setPrimeLineNO(String primeLineNO) {
		this.primeLineNO = primeLineNO;
	}

	public String getReqShipDate() {
		return reqShipDate;
	}

	public void setReqShipDate(String reqShipDate) {
		this.reqShipDate = reqShipDate;
	}

	public String getReqDeliveryDate() {
		return reqDeliveryDate;
	}

	public void setReqDeliveryDate(String reqDeliveryDate) {
		this.reqDeliveryDate = reqDeliveryDate;
	}

	public String getItemID() {
		return itemID;
	}

	public void setItemID(String itemID) {
		this.itemID = itemID;
	}

	public String getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(String unitPrice) {
		this.unitPrice = unitPrice;
	}

	public String getOrderedQty() {
		return orderedQty;
	}

	public void setOrderedQty(String orderedQty) {
		this.orderedQty = orderedQty;
	}

	public String getUom() {
		return uom;
	}

	public void setUom(String uom) {
		this.uom = uom;
	}

	public String getEarliestDeliveryDate() {
		return earliestDeliveryDate;
	}

	public void setEarliestDeliveryDate(String earliestDeliveryDate) {
		this.earliestDeliveryDate = earliestDeliveryDate;
	}

	public String getEarliestShipDate() {
		return earliestShipDate;
	}

	public void setEarliestShipDate(String earliestShipDate) {
		this.earliestShipDate = earliestShipDate;
	}

	public String getShipNode() {
		return shipNode;
	}

	public void setShipNode(String shipNode) {
		this.shipNode = shipNode;
	}

	public String getShipToID() {
		return shipToID;
	}

	public void setShipToID(String shipToID) {
		this.shipToID = shipToID;
	}

	public String getLineTotal() {
		return lineTotal;
	}

	public void setLineTotal(String lineTotal) {
		this.lineTotal = lineTotal;
	}
	
	public String getOrderLineKey() {
		return orderLineKey;
	}

	public void setOrderLineKey(String orderLineKey) {
		this.orderLineKey = orderLineKey;
	}

	public String getOrderHeaderKey() {
		return orderHeaderKey;
	}

	public void setOrderHeaderKey(String orderHeaderKey) {
		this.orderHeaderKey = orderHeaderKey;
	}

}
