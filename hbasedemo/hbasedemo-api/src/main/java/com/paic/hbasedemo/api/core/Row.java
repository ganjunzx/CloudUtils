package com.paic.hbasedemo.api.core;

import java.util.Arrays;

public class Row {
	private byte[] key;
	private byte[] value;
	
	public Row(byte[] key, byte[] value) {
		this.key = key;
		this.value = value;
	}
	
	public byte[] getKey() {
		return this.key;
	}
	
	public byte[] getValue() {
		return this.value;
	}
	
	public Row copyRow() {
		byte[] keyTemp = Arrays.copyOf(key, key.length);
		byte[] valueTemp = Arrays.copyOf(value, value.length);
		return new Row(keyTemp, valueTemp);
	}
}
