package rabbitMQ;

import java.io.Serializable;

public class Request implements Serializable{

	String type;
	Object payload;
	
	public String getType() {
		return type;
	}
	
	public Object getPayload() {
		return payload;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public void setPayload(Object payload) {
		this.payload =  payload;
	}
	
}
