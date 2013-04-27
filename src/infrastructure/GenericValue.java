package infrastructure;

import org.apache.hadoop.io.GenericWritable;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public class GenericValue extends GenericWritable {

	private static Class[] CLASSES = { VertexValue.class, Message.class, };

	@Override
	protected Class[] getTypes() {
		return CLASSES;
	}

	@Override
	public String toString() {
		EmitInterface ei = (EmitInterface) this.get();
		if (ei.getEmitType() == EmitType.VERTEX) {
			return ((VertexValue) ei).toString();
		} else if (ei.getEmitType() == EmitType.MESSAGE) {
			return ((Message) ei).toString();
		} else {
			return null;
		}

	}

}
