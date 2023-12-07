package com.speedment.livedata.data.types;

import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

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
	private JSONObject lineRootJSON;
	private JSONObject lineDataJSON;

	public OrderLine(List<String> columnList, List<String> valueList) throws Exception {
		initializeOrderLineData(columnList, valueList);
		generateLineJsonObject();
	}

	private void initializeOrderLineData(List<String> columnList, List<String> valueList) {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(
					valueList.get(index), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
			switch (sTableColumn) {
				
				case LiveDataConsts.OMS_ORDER_LINE_KEY:				
					orderLineKey = value;
					break;
				
				case LiveDataConsts.OMS_ORDER_HEADER_KEY:				
					orderHeaderKey = value;
					break;
				
				case LiveDataConsts.OMS_PRIME_LINE_NO:				
					primeLineNO = value;
					break;
				
				case LiveDataConsts.OMS_REQ_DELIVERY_DATE:				
					reqDeliveryDate = LiveDataUtils.formatDate(value);
					break;
				
				case LiveDataConsts.OMS_REQ_SHIP_DATE:				
					reqShipDate = LiveDataUtils.formatDate(value);
					break;
				
				case LiveDataConsts.OMS_UNIT_PRICE:				
					unitPrice = value;
					break;
				
				case LiveDataConsts.OMS_ITEM_ID:				
					itemID = value;
					break;
				
				case LiveDataConsts.OMS_ORDERED_QTY:				
					orderedQty = value;
					break;
					
				case LiveDataConsts.OMS_UOM:				
					uom = value;
					break;
					
				case LiveDataConsts.OMS_EARLIEST_DELIVERY_DATE:				
					earliestDeliveryDate = LiveDataUtils.formatDate(value);
					break;
					
				case LiveDataConsts.OMS_EARLIEST_SHIP_DATE:				
					earliestShipDate = LiveDataUtils.formatDate(value);
					break;
					
				case LiveDataConsts.OMS_SHIP_ID:				
					shipToID = value;
					break;
					
				case LiveDataConsts.OMS_SHIP_NODE_KEY:				
					shipNode = value;
					break;	
					
				case LiveDataConsts.OMS_LINE_TOTAL:				
					lineTotal = value;
					break;	
				
				default:
					break;
			}			
			index++;	
		}
	}

	
	private void generateLineJsonObject() throws Exception {
			
			lineRootJSON = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.SCIS_TYPE_ORDER);
			JSONObject businessObject = lineRootJSON
					.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
					.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 

			lineDataJSON = new JSONObject();
			businessObject.put(LiveDataConsts.SCIS_ORDER_LINES, new JSONArray().put(lineDataJSON));
			
			//appending line data
			lineDataJSON.put(LiveDataConsts.SCIS_ORDER_LINE_NO, getPrimeLineNO());				
			lineDataJSON.put(LiveDataConsts.SCIS_REQ_DELIVERY_DATE, getReqDeliveryDate());
			lineDataJSON.put(LiveDataConsts.SCIS_REQ_SHIP_DATE, getReqShipDate());
			lineDataJSON.put(LiveDataConsts.SCIS_PRODUCT_VALUE,getUnitPrice());				
			lineDataJSON.put(LiveDataConsts.SCIS_QUANTITY, getOrderedQty());				
			lineDataJSON.put(LiveDataConsts.SCIS_QUANTITY_UNITS, getUom());				
			lineDataJSON.put(LiveDataConsts.SCIS_PLANNED_DEL_DATE, getEarliestDeliveryDate());				
			lineDataJSON.put(LiveDataConsts.SCIS_PLANNED_SHIP_DATE, getEarliestShipDate());
			lineDataJSON.put(LiveDataConsts.SCIS_VALUE, getLineTotal());
			
			lineDataJSON.put(LiveDataConsts.SCIS_PRODUCT, 
					LiveDataUtils.createGlobalIdentifier(getItemID()));
			
			lineDataJSON.put(LiveDataConsts.SCIS_SHIP_FROM_INSTR_LOCATION, 
					LiveDataUtils.createGlobalIdentifier(getShipNode()));
					
			lineDataJSON.put(LiveDataConsts.SCIS_SHIP_TO_LOCATION, 
					LiveDataUtils.createGlobalIdentifier(getShipToID()));
		
	}
	
	public void updateLineJSONwithOrderData(OrderHeader orderObj) throws Exception {
		JSONObject businessObject = lineRootJSON
				.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
				.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
		
		businessObject.put(LiveDataConsts.SCIS_ORDER_TYPE, LiveDataUtils.getOrderType(orderObj.getDocumentType()));
		businessObject.put(LiveDataConsts.SCIS_ORDER_IDENTIFIER, orderObj.getOrderNo());
		LiveDataUtils.createRootGlobalIdentifier(businessObject, 
						LiveDataUtils.getOrderNumber(orderObj.getOrderNo(), orderObj.getDocumentType()));
				
		LiveDataUtils.createRootGlobalIdentifier(businessObject, 
						LiveDataUtils.getOrderNumber(orderObj.getOrderNo(), orderObj.getDocumentType()));
				
				//line related order data
		lineDataJSON.put(LiveDataConsts.SCIS_CREATED_DATE, orderObj.getOrderDate());
		lineDataJSON.put(LiveDataConsts.SCIS_VALUE_CURRENCY, orderObj.getCurrency());
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
	
	public JSONObject getLineRootJSON() {
		return lineRootJSON;
	}

	public void setLineRootJSON(JSONObject lineRootJSON) {
		this.lineRootJSON = lineRootJSON;
	}

	public JSONObject getLineDataJSON() {
		return lineDataJSON;
	}

	public void setLineDataJSON(JSONObject lineDataJSON) {
		this.lineDataJSON = lineDataJSON;
	}

}
