package infrastructure;

import java.io.Serializable;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public enum EmitType implements Serializable {

	VERTEX, MESSAGE;

	@Override
	public String toString() {
		switch (this) {
		case VERTEX:
			return "Vertex";
		case MESSAGE:
			return "Message";
		default:
			return null;
		}
	}

}
