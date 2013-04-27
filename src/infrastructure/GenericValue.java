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

}
