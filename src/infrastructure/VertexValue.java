package infrastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import core.CommonConstants;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public class VertexValue implements Writable, EmitInterface {

	/**
	 * The current minimum distance of this vertex from the source vertex.
	 */
	private double discoveryDistance = Double.MAX_VALUE;

	/**
	 * The social capital value of the current vertex.
	 */
	private double scv = 0d;

	private boolean active = false;

	/**
	 * The adjacency list of the vertex, with the destination id as the key and
	 * edge weight as the value.
	 */
	private Map<Integer, Double> adjacencyList = new HashMap<Integer, Double>();

	private Map<BPMessageKey, Integer> activeIncomingEdges = new HashMap<BPMessageKey, Integer>();

	private Map<Integer, Integer> hopPacketCountMap = new HashMap<Integer, Integer>();

	public double getDiscoveryDistance() {
		return discoveryDistance;
	}

	public void setDiscoveryDistance(double discoveryDistance) {
		this.discoveryDistance = discoveryDistance;
	}

	public double getScv() {
		return scv;
	}

	public void setScv(double scv) {
		this.scv = scv;
	}

	public Map<Integer, Double> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(Map<Integer, Double> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public Map<BPMessageKey, Integer> getActiveIncomingEdges() {
		return activeIncomingEdges;
	}

	public void setActiveIncomingEdges(
			Map<BPMessageKey, Integer> activeIncomingEdges) {
		this.activeIncomingEdges = activeIncomingEdges;
	}

	public void resetTemproryValues() {
		this.discoveryDistance = Double.MAX_VALUE;
		this.activeIncomingEdges.clear();
		hopPacketCountMap.clear();
		this.active = false;
	}

	public void readFields(DataInput dataInputStream) throws IOException {
		this.discoveryDistance = dataInputStream.readDouble();
		this.scv = dataInputStream.readDouble();
		this.active = dataInputStream.readBoolean();

		int start = dataInputStream.readUTF().trim().indexOf("{");
		int stop = dataInputStream.readUTF().trim().indexOf("}");
		String adjListString = dataInputStream.readUTF().substring(start + 1,
				stop);
		Map<Integer, Double> adjacencyList = new HashMap<Integer, Double>();
		if (!adjListString.isEmpty()) {
			String[] adjList = adjListString.split(",");
			for (int i = 0; i < adjList.length; i += 2) {
				adjacencyList.put(Integer.parseInt(adjList[i]), Double
						.parseDouble(adjList[i + 1]));
			}
		}
		this.adjacencyList = adjacencyList;

		start = dataInputStream.readUTF().trim().indexOf("{");
		stop = dataInputStream.readUTF().trim().indexOf("}");
		String bpMsgString = dataInputStream.readUTF().substring(start + 1,
				stop);
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
		this.activeIncomingEdges = bpMsgList;

		start = dataInputStream.readUTF().trim().indexOf("{");
		stop = dataInputStream.readUTF().trim().indexOf("}");
		String hopPktCntString = dataInputStream.readUTF().substring(start + 1,
				stop);
		Map<Integer, Integer> hopPktCntMap = new HashMap<Integer, Integer>();
		if (!hopPktCntString.isEmpty()) {
			String[] hpcString = hopPktCntString.split(",");
			for (int i = 0; i < hpcString.length; i += 2) {
				int k = Integer.parseInt(hpcString[i]);
				hopPktCntMap.put(k, Integer.parseInt(hpcString[i + 1]));
			}
		}
		this.hopPacketCountMap = hopPktCntMap;

	}

	public void write(DataOutput dataOutputStream) throws IOException {
		dataOutputStream.writeDouble(this.discoveryDistance);
		dataOutputStream.writeDouble(this.scv);
		dataOutputStream.writeBoolean(this.active);
		StringBuffer buf = new StringBuffer();
		buf.append("{");
		for (Iterator<Integer> adjListIter = adjacencyList.keySet().iterator(); adjListIter
				.hasNext();) {
			int adjId = adjListIter.next();
			double wt = adjacencyList.get(adjId);
			buf.append(Integer.toString(adjId));
			buf.append(CommonConstants.COMMA);
			buf.append(Double.toString(wt));
		}
		buf.append("}");
		dataOutputStream.writeUTF(buf.toString());

		StringBuffer buf2 = new StringBuffer();
		buf.append("{");
		for (Iterator<BPMessageKey> incEdgeIter = activeIncomingEdges.keySet()
				.iterator(); incEdgeIter.hasNext();) {
			BPMessageKey k = incEdgeIter.next();
			int v = activeIncomingEdges.get(k);
			buf2.append(k.toString());
			buf2.append(CommonConstants.COMMA);
			buf2.append(Integer.toString(v));
		}
		buf2.append("}");
		dataOutputStream.writeUTF(buf2.toString());

		StringBuffer buf3 = new StringBuffer();
		buf.append("{");
		for (Iterator<Integer> adjListIter = hopPacketCountMap.keySet()
				.iterator(); adjListIter.hasNext();) {
			int adjId = adjListIter.next();
			int pkts = hopPacketCountMap.get(adjId);
			buf3.append(Integer.toString(adjId));
			buf3.append(CommonConstants.COMMA);
			buf3.append(Integer.toString(pkts));
		}
		buf3.append("}");
		dataOutputStream.writeUTF(buf.toString());

	}

	public EmitType getEmitType() {
		return EmitType.VERTEX;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public boolean isActive() {
		return active;
	}

	public void setHopPacketCountMap(Map<Integer, Integer> hopPacketCountMap) {
		this.hopPacketCountMap = hopPacketCountMap;
	}

	public Map<Integer, Integer> getHopPacketCountMap() {
		return hopPacketCountMap;
	}

	public void addIncomingMessage(Message incomingMessage) {
		BPMessageKey bpMsgKey = new BPMessageKey(incomingMessage.getSourceId(),
				incomingMessage.getHops());
		if (incomingMessage.getDistance() < this.discoveryDistance) {
			this.resetTemproryValues();
			this.activeIncomingEdges
					.put(bpMsgKey, incomingMessage.getPackets());
			this.hopPacketCountMap.put(incomingMessage.getHops(),
					incomingMessage.getPackets());
		} else if (incomingMessage.getDistance() == this.discoveryDistance) {
			this.activeIncomingEdges
					.put(bpMsgKey, incomingMessage.getPackets());
			this.hopPacketCountMap.put(incomingMessage.getHops(),
					this.hopPacketCountMap.get(incomingMessage.getHops())
							+ incomingMessage.getPackets());
		}
	}
}
