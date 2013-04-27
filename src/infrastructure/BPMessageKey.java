package infrastructure;

import java.io.Serializable;

import core.CommonConstants;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public class BPMessageKey implements Serializable {

	public int sourceNodeId;

	public int hop;

	public BPMessageKey() {

	}

	public BPMessageKey(int s, int h) {
		this.sourceNodeId = s;
		this.hop = h;
	}

	@Override
	public int hashCode() {
		return sourceNodeId + hop;
	}

	@Override
	public boolean equals(Object o) {
		BPMessageKey k = (BPMessageKey) o;
		if (this.sourceNodeId - k.sourceNodeId == 0 && this.hop - k.hop == 0) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(Integer.toString(this.sourceNodeId));
		buf.append(CommonConstants.COMMA);
		buf.append(Integer.toString(this.hop));
		return buf.toString();
	}

}
