package application;

public class Sensor implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String temperature;
	public Integer value;
	
	public Sensor(String temperature, Integer value) {
		this.temperature = temperature;
		this.value = value;
	}
}
