package net.nuttle.hbase;

public class HBaseException extends Exception {

	public HBaseException(String msg) {
		super(msg);
	}
	
	public HBaseException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
