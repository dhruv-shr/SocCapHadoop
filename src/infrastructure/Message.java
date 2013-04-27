package infrastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public class Message extends GenericValue implements Writable {

	private int sourceId;

	private int hops;

	private int packets;

	private double distance;

	public Message() {

	}

	public Message(int sourceId, int hops, int packets, double distance) {
		this.sourceId = sourceId;
		this.hops = hops;
		this.packets = packets;
		this.distance = distance;
	}

	public int getSourceId() {
		return sourceId;
	}

	public void setSourceId(int sourceId) {
		this.sourceId = sourceId;
	}

	public int getHops() {
		return hops;
	}

	public void setHops(int hops) {
		this.hops = hops;
	}

	public int getPackets() {
		return packets;
	}

	public void setPackets(int packets) {
		this.packets = packets;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public void readFields(DataInput dataInputStream) throws IOException {
		this.sourceId = dataInputStream.readInt();
		this.hops = dataInputStream.readInt();
		this.packets = dataInputStream.readInt();
		this.distance = dataInputStream.readDouble();
	}

	public void write(DataOutput dataOutputStream) throws IOException {
		dataOutputStream.writeInt(this.sourceId);
		dataOutputStream.writeInt(this.hops);
		dataOutputStream.writeInt(this.packets);
		dataOutputStream.writeDouble(distance);
	}

	/**
	 * This returns the emit type of this particular class. Required because the
	 * mapper will emit two different kinds of value. Namely VertexValue and
	 * Message.
	 */
	public EmitType getEmitType() {
		/**
		 * This class is a message class and will emit a Message Class object.
		 */
		return EmitType.MESSAGE;
	}

}
