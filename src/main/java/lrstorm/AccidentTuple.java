package lrstorm;

import java.io.Serializable;

import backtype.storm.tuple.Values;

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

	public Values toValues() {
		return new Values(segment, isAccident, ts);
	}

	@Override
	public String toString() {
		return segment + "," + isAccident + "," + ts;
	}

}