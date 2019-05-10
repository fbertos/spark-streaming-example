package org.fbertos.spark;

public class Message {
   private Schema schema;
   private String payload;
public Schema getSchema() {
	return schema;
}
public void setSchema(Schema schema) {
	this.schema = schema;
}
public String getPayload() {
	return payload;
}
public void setPayload(String payload) {
	this.payload = payload;
}
}
