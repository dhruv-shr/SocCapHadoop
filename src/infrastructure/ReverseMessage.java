package infrastructure;

import core.CommonConstants;

public class ReverseMessage {

	int sourceVertex;
	int hops;
	double distance;
	int packets;
	int totalHops;
	
	public ReverseMessage(){
		
	}
	
	public ReverseMessage(int sourveVertex, double distance , int hops , int packets, int totalHops){
		this.sourceVertex = sourveVertex;
		this.distance = distance;
		this.hops = hops;
		this.packets = packets;
		this.totalHops = totalHops;
	}
	
	public static String getMessageString(ReverseMessage m){
		return m.getSourceVertex() +"	" +
	           m.getDistance() + "	" +
			   m.getHops() +"	" +
	           m.getPackets() + "	" +
			   m.getTotalHops();
	}
	
	public static ReverseMessage getReverseMessageObject(String str){
		String reverseMessageObject[] = str.split(CommonConstants.TAB);
		ReverseMessage reverseMessage = new ReverseMessage();
		reverseMessage.sourceVertex = Integer.parseInt(reverseMessageObject[0]);
		reverseMessage.distance = Double.parseDouble(reverseMessageObject[1]);
		reverseMessage.hops = Integer.parseInt(reverseMessageObject[2]);
		reverseMessage.packets = Integer.parseInt(reverseMessageObject[3]);
		reverseMessage.totalHops = Integer.parseInt(reverseMessageObject[4]);
		
		return reverseMessage;
	}
	
	public static double computeSCV(ReverseMessage reverseMessage){
		
		   double scv = (reverseMessage.packets * (Math.exp(-1 * 1/ reverseMessage.distance) / ((reverseMessage.totalHops + 1))));
		   return scv;
    }
	
	public int getSourceVertex() {
		return sourceVertex;
	}
	public void setSourceVertex(int sourceVertex) {
		this.sourceVertex = sourceVertex;
	}
	public int getHops() {
		return hops;
	}
	public void setHops(int hops) {
		this.hops = hops;
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	public int getPackets() {
		return packets;
	}
	public void setPackets(int packets) {
		this.packets = packets;
	}

	public int getTotalHops() {
		return totalHops;
	}

	public void setTotalHops(int totalHops) {
		this.totalHops = totalHops;
	}
	
	
	
}
