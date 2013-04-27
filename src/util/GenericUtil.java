package util;

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

}
