package com.speedment.livedata.data.types;

import java.net.URLDecoder;
import java.util.List;
import org.json.JSONObject;

import com.ibm.cloud.objectstorage.util.StringUtils;
import com.speedment.livedata.global.LiveDataConsts;
import com.speedment.livedata.global.LiveDataUtils;

public class PersonInfo {

	String personInfoKey;
	String personID;
	String firstName;
	String lastName;
	String addressLine1;
	String addressLine2;
	String city;
	String state;
	String zipCode;
	String country;
	String dayPhone;
	String mobilePhone;
	String latitude;
	String longitude;
	String addressType;
	JSONObject personInfoJSON;

	public PersonInfo(List<String> columnList, List<String> valueList) throws Exception {
		initializeOrgData(columnList, valueList);
		generatePersonInfoJSON();
	}

	private void initializeOrgData(List<String> columnList, List<String> valueList) throws Exception {
		int index = 0;
		for (String sTableColumn : columnList){			
			String value = LiveDataUtils.removeUnwantedCharacters(
					valueList.get(index), LiveDataConsts.SINGLE_QUOTE, LiveDataConsts.NO_SPACE);
			
			switch (sTableColumn) {
				case LiveDataConsts.OMS_PERSON_INFO_KEY:				
					personInfoKey = value;
					break;
				
				case LiveDataConsts.OMS_PERSON_ID:				
					personID = value;
					break;
				
				case LiveDataConsts.OMS_FIRST_NAME:				
					firstName = value;
					break;
				case LiveDataConsts.OMS_LAST_NAME:				
					lastName = value;
					break;
				
				case LiveDataConsts.OMS_DAY_PHONE:				
					dayPhone = value;
					break;
					
				case LiveDataConsts.OMS_MOBILE_PHONE:				
					mobilePhone = value;
					break;
					
				case LiveDataConsts.OMS_ADDR_LINE_1:
				{
					addressLine1 = URLDecoder.decode(value, "UTF-8");
				}					
				break;
					
				case LiveDataConsts.OMS_ADDR_LINE_2:				
					addressLine2 = value;
					break;
					
				case LiveDataConsts.OMS_CITY:				
					city = value;
					break;
					
				case LiveDataConsts.OMS_STATE:				
					state = value;
					break;
				
				case LiveDataConsts.OMS_ZIP_CODE:				
					zipCode = value;
					break;
				
				case LiveDataConsts.SCIS_COUNTRY:				
					country = value;
					break;
					
				case LiveDataConsts.OMS_LONGITUDE:				
					longitude = value;
					break;
					
				case LiveDataConsts.OMS_LATITUDE:				
					latitude = value;
					break;
					
				case LiveDataConsts.OMS_IS_COMM_ADD:
					addressType = (value == "Y")? "SUPPLIER": "BUYER";
					break;
					
				default:
					break;
				}			
			index++;	
		}	
		
	}
	
	
	private void generatePersonInfoJSON() throws Exception {
	
		personInfoJSON = LiveDataUtils.createRootJsonForCOS(LiveDataConsts.SCIS_LOCATION);
		JSONObject businessObject = personInfoJSON
					.getJSONObject(LiveDataConsts.SCIS_EVENT_DETAILS)
					.getJSONObject(LiveDataConsts.SCIS_BUSINESS_OBJECT); 
		
		businessObject.put(LiveDataConsts.SCIS_ADDRESS1, addressLine1);
		businessObject.put(LiveDataConsts.SCIS_CITY, city);
		businessObject.put(LiveDataConsts.SCIS_COORDINATES, 
								latitude.concat(", ").concat(longitude));
		businessObject.put(LiveDataConsts.SCIS_COUNTRY, StringUtils.isNullOrEmpty(country)? "USA": country );
		businessObject.put(LiveDataConsts.SCIS_GEO, "AMERICAS");
		
		LiveDataUtils.createRootGlobalIdentifier(businessObject, firstName);
		
		businessObject.put(LiveDataConsts.SCIS_INCLUDE_IN_CORRELATION, true);
		businessObject.put(LiveDataConsts.SCIS_LOCATION_IDENTIFIER, 
				StringUtils.isNullOrEmpty(personID)? firstName: personID );
		businessObject.put(LiveDataConsts.SCIS_LOCATION_NAME, firstName);
		businessObject.put(LiveDataConsts.SCIS_LOCATION_TYPE, addressType);
		businessObject.put(LiveDataConsts.SCIS_POSTAL_CODE, zipCode);
		businessObject.put(LiveDataConsts.SCIS_STATE_PROV, state);

	}
	
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getAddressLine1() {
		return addressLine1;
	}
	public void setAddressLine1(String addressLine1) {
		this.addressLine1 = addressLine1;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getZipCode() {
		return zipCode;
	}
	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getDayPhone() {
		return dayPhone;
	}
	public void setDayPhone(String dayPhone) {
		this.dayPhone = dayPhone;
	}
	public String getMobilePhone() {
		return mobilePhone;
	}
	public void setMobilePhone(String mobileNo) {
		this.mobilePhone = mobileNo;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	
	public String getAddressLine2() {
		return addressLine2;
	}

	public void setAddressLine2(String addressLine2) {
		this.addressLine2 = addressLine2;
	}

	
	public JSONObject getPersonInfoJSON() {
		return personInfoJSON;
	}

	public void setPersonInfoJSON(JSONObject personInfoJSON) {
		this.personInfoJSON = personInfoJSON;
	}
	public String getPersonID() {
		return personID;
	}

	public void setPersonID(String personID) {
		this.personID = personID;
	}

	public String getAddressType() {
		return addressType;
	}

	public void setAddressType(String type) {
		this.addressType = type;
	}
	public String getPersonInfoKey() {
		return personInfoKey;
	}

	public void setPersonInfoKey(String personInfoKey) {
		this.personInfoKey = personInfoKey;
	}

}
