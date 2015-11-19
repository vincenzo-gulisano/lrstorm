package lrstorm;

import java.io.Serializable;

public class AccidentTuple implements Serializable {
	
	private static final long serialVersionUID = 6655042762837984179L;

	public String segment;
	public boolean isAccident;
	public long ts;

	public AccidentTuple(String segment, boolean isAccident, long ts) {
		this.segment = segment;
		this.isAccident = isAccident;
		this.ts = ts;
	}

	@Override
	public String toString() {
		return segment + "," + isAccident + "," + ts;
	}

}