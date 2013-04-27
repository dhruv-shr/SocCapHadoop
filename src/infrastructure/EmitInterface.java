package infrastructure;

/**
 * This interface helps the Mapper emit different kind of values. Reducer is
 * able to distinguish between these values.
 * 
 * 
 * @author dhruvsharma1
 * 
 */
public interface EmitInterface {

	public EmitType getEmitType();

}
