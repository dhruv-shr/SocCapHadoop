package util;

import java.util.HashMap;
import java.util.Map;

import core.CommonConstants;
import infrastructure.BPMessageKey;
import infrastructure.EmitInterface;
import infrastructure.EmitType;
import infrastructure.GenericValue;
import infrastructure.Message;
import infrastructure.VertexValue;

public class GenericUtil {

	/**
	 * This function converts the given EmitInterface subclass object to
	 * GenericValue type. This is to be handeled by the implementor totally.
	 * 
	 * @param val
	 * @return
	 */
	public static GenericValue makeGeneric(EmitInterface val) {
		GenericValue genericValue = new GenericValue();
		if (val.getEmitType().equals(EmitType.MESSAGE)) {
			genericValue.set((Message) val);
		} else if (val.getEmitType().equals(EmitType.VERTEX)) {
			genericValue.set((VertexValue) val);
		}
		return genericValue;
	}
	
public static String getVertexValueString(VertexValue vertex){
		
		//Vertex Id Map
		String id = "" + vertex.getVertexID() ;
		
        String flag = "" ;
		if(vertex.isActive()){
			flag = "true";
		}else{
			flag = "false";
		}
		
		//Discovery Distance Calculations
		String discoveryDistance = "" +  vertex.getDiscoveryDistance();
		
		//Social Capital Value
		String socialCapitalValue = "" + vertex.getScv();
		
		
		//Adjacency List Creation
		String adjacencyListString = "";
		Map<Integer, Double> adjacencyList = vertex.getAdjacencyList();
		for(Integer i : adjacencyList.keySet()){
			adjacencyListString = adjacencyListString + "" + i + "," + adjacencyList.get(i) +"," ;
		}
		
		if(adjacencyListString.length() > 0)
		adjacencyListString = adjacencyListString.substring(0, adjacencyListString.length()-1);
		
		adjacencyListString = "{" + adjacencyListString + "}" ;
		
		//BackPropogationSTring
		String backPropogationString = "";
		Map<BPMessageKey, Integer> bpMsgList = vertex.getActiveIncomingEdges();
		for(BPMessageKey bpMessaageKey : bpMsgList.keySet()){
			backPropogationString = backPropogationString + bpMessaageKey.sourceNodeId + 
					         "," + bpMessaageKey.hop + "," + bpMsgList.get(bpMessaageKey) +",";	
		}
		
		if(backPropogationString.length() > 0)
		backPropogationString = backPropogationString.substring(0,backPropogationString.length()-1);
		backPropogationString = "{" + backPropogationString +"}";
		
		//hopCountString
		String hopCountString = "";
		Map<Integer, Integer> hopPktCntMap = vertex.getHopPacketCountMap();
		for(Integer i : hopPktCntMap.keySet()){
			hopCountString = hopCountString + i +"," + hopPktCntMap.get(i)+",";
		}
		
		if(hopCountString.length() > 0)
		hopCountString = hopCountString.substring(0,hopCountString.length()-1);
		
		hopCountString = "{" + hopCountString + "}";
		
		String result = id + "	" + flag + "	" + discoveryDistance + "	" +
		                socialCapitalValue + "	" + adjacencyListString + "	" +
		                 backPropogationString + "	"  + hopCountString;
		
		return result;
				
		
	}

public static String getVertexValueStringFinalPhase(VertexValue vertex){
	
	//Vertex Id Map
	String id = "" + vertex.getVertexID() ;
	
    String flag = "" ;
	if(vertex.isActive()){
		flag = "true";
	}else{
		flag = "false";
	}
	
	//Discovery Distance Calculations
	String discoveryDistance = "" +  vertex.getDiscoveryDistance();
	
	//Social Capital Value
	String socialCapitalValue = "" + vertex.getScv();
	
	
	//Adjacency List Creation
	String adjacencyListString = "";
	Map<Integer, Double> adjacencyList = vertex.getAdjacencyList();
	for(Integer i : adjacencyList.keySet()){
		adjacencyListString = adjacencyListString + "" + i + "," + adjacencyList.get(i) +"," ;
	}
	
	if(adjacencyListString.length() > 0)
	adjacencyListString = adjacencyListString.substring(0, adjacencyListString.length()-1);
	
	adjacencyListString = "{" + adjacencyListString + "}" ;
	
	//BackPropogationSTring
	String backPropogationString = "";
	Map<BPMessageKey, Integer> bpMsgList = vertex.getActiveIncomingEdges();
	for(BPMessageKey bpMessaageKey : bpMsgList.keySet()){
		backPropogationString = backPropogationString + bpMessaageKey.sourceNodeId + 
				         "," + bpMessaageKey.hop + "," + bpMsgList.get(bpMessaageKey) +",";	
	}
	
	if(backPropogationString.length() > 0)
	backPropogationString = backPropogationString.substring(0,backPropogationString.length()-1);
	backPropogationString = "{" + backPropogationString +"}";
	
	//hopCountString
	String hopCountString = "";
	Map<Integer, Integer> hopPktCntMap = vertex.getHopPacketCountMap();
	for(Integer i : hopPktCntMap.keySet()){
		hopCountString = hopCountString + i +"," + hopPktCntMap.get(i)+",";
	}
	
	if(hopCountString.length() > 0)
	hopCountString = hopCountString.substring(0,hopCountString.length()-1);
	
	hopCountString = "{" + hopCountString + "}";
	
	String result = flag + "	" + discoveryDistance + "	" +
	                socialCapitalValue + "	" + adjacencyListString + "	" +
	                 backPropogationString + "	"  + hopCountString;
	
	return result;
			
	
}
	
public static VertexValue getVertexObject(String inputLine) throws Exception{
		
		String splits[] = inputLine.split(
				CommonConstants.TAB);
		VertexValue vertexValue = new VertexValue();
		vertexValue.setVertexID(Integer.parseInt(splits[0]));
		vertexValue.setActive(Boolean.parseBoolean(splits[1]));
		vertexValue.setDiscoveryDistance(Double.parseDouble(splits[2]));
		vertexValue.setScv(Double.parseDouble(splits[3]));
		
		int start = splits[4].trim().indexOf("{");
		int stop = splits[4].trim().indexOf("}");
		String adjListString = splits[4].substring(start + 1, stop);
		
		
		Map<Integer, Double> adjacencyList = new HashMap<Integer, Double>();
		if (!adjListString.isEmpty()) {
			String[] adjList = adjListString.split(",");
			for (int i = 0; i < adjList.length; i += 2) {
				adjacencyList.put(Integer.parseInt(adjList[i]), Double
						.parseDouble(adjList[i + 1]));
			}
		}
		
		vertexValue.setAdjacencyList(adjacencyList);
		start = splits[5].trim().indexOf("{");
		stop = splits[5].trim().indexOf("}");
		String bpMsgString = splits[5].substring(start + 1, stop);
		
		Map<BPMessageKey, Integer> bpMsgList = new HashMap<BPMessageKey, Integer>();
		
		if (!bpMsgString.isEmpty()) {
			String[] bpMsgListString = bpMsgString.split(",");
			for (int i = 0; i < bpMsgListString.length; i += 3) {
				BPMessageKey k = new BPMessageKey(Integer
						.parseInt(bpMsgListString[i]), Integer
						.parseInt(bpMsgListString[i + 1]));
				bpMsgList.put(k, Integer.parseInt(bpMsgListString[i + 2]));
			}
		}
		vertexValue.setActiveIncomingEdges(bpMsgList);

		start = splits[6].trim().indexOf("{");
		stop = splits[6].trim().indexOf("}");
		String hopPktCntString = splits[6].substring(start + 1, stop);
		Map<Integer, Integer> hopPktCntMap = new HashMap<Integer, Integer>();
		if (!hopPktCntString.isEmpty()) {
			String[] hpcString = hopPktCntString.split(",");
			for (int i = 0; i < hpcString.length; i += 2) {
				int k = Integer.parseInt(hpcString[i]);
				hopPktCntMap.put(k, Integer.parseInt(hpcString[i + 1]));
			}
		}
		vertexValue.setHopPacketCountMap(hopPktCntMap);

		return vertexValue;
	}

}
