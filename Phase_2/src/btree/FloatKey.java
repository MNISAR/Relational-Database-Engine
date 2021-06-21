package btree;

public class FloatKey extends KeyClass {

	private Float key;

	public String toString() {
		return key.toString();
	}

	public FloatKey(Float value) {
		key = new Float(value.floatValue());
	}

	public FloatKey(float value) {
		key = new Float(value);
	}

	/**
	 * get a copy of the float key
	 * 
	 * @return the reference of the copy
	 */
	public Float getKey() {
		return new Float(key.floatValue());
	}

	/**
	 * set the float key value
	 */
	public void setKey(Float value) {
		key = new Float(value.floatValue());
	}

}
