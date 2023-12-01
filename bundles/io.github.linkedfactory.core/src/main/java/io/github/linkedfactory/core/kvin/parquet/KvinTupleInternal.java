package io.github.linkedfactory.core.kvin.parquet;

import java.util.Arrays;

public class KvinTupleInternal implements Comparable<KvinTupleInternal> {
	protected byte[] id;
	protected Long time;
	protected Integer seqNr;
	protected Integer valueInt;
	protected Long valueLong;
	protected Float valueFloat;
	protected Double valueDouble;
	protected String valueString;
	protected Integer valueBool;
	protected byte[] valueObject;

	public byte[] getId() {
		return id;
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getSeqNr() {
		return seqNr;
	}

	public void setSeqNr(int seqNr) {
		this.seqNr = seqNr;
	}

	public Integer getValueInt() {
		return valueInt;
	}

	public void setValueInt(Integer valueInt) {
		this.valueInt = valueInt;
	}

	public Long getValueLong() {
		return valueLong;
	}

	public void setValueLong(Long valueLong) {
		this.valueLong = valueLong;
	}

	public Float getValueFloat() {
		return valueFloat;
	}

	public void setValueFloat(Float valueFloat) {
		this.valueFloat = valueFloat;
	}

	public Double getValueDouble() {
		return valueDouble;
	}

	public void setValueDouble(Double valueDouble) {
		this.valueDouble = valueDouble;
	}

	public String getValueString() {
		return valueString;
	}

	public void setValueString(String valueString) {
		this.valueString = valueString;
	}

	public byte[] getValueObject() {
		return valueObject;
	}

	public void setValueObject(byte[] valueObject) {
		this.valueObject = valueObject;
	}

	public Integer getValueBool() {
		return valueBool;
	}

	public void setValueBool(Integer valueBool) {
		this.valueBool = valueBool;
	}

	@Override
	public int compareTo(KvinTupleInternal o) {
		int diff = Arrays.compare(id, o.id);
		if (diff != 0) {
			return diff;
		}
		diff = time.compareTo(o.time);
		if (diff != 0) {
			// time is reverse
			return -diff;
		}
		diff = seqNr.compareTo(o.seqNr);
		if (diff != 0) {
			// seqNr is reverse
			return -diff;
		}
		return 0;
	}
}