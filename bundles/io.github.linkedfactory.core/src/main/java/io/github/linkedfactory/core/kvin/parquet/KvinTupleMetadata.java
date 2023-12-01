package io.github.linkedfactory.core.kvin.parquet;

public class KvinTupleMetadata {
	String item;
	String property;
	long itemId;
	long propertyId;
	long contextId;

	public KvinTupleMetadata(String item, String property, long itemId, long propertyId, long contextId) {
		this.item = item;
		this.property = property;
		this.itemId = itemId;
		this.propertyId = propertyId;
		this.contextId = contextId;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	public long getItemId() {
		return itemId;
	}

	public void setItemId(long itemId) {
		this.itemId = itemId;
	}

	public long getPropertyId() {
		return propertyId;
	}

	public void setPropertyId(long propertyId) {
		this.propertyId = propertyId;
	}

	public long getContextId() {
		return contextId;
	}

	public void setContextId(long contextId) {
		this.contextId = contextId;
	}
}