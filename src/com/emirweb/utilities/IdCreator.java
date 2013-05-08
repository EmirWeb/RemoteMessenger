package com.emirweb.utilities;

public class IdCreator {

	private long id = -1;
	
	public synchronized long getNewId(){
		return ++id;
	}
	
	public synchronized long getCurrentId(){
		return id;
	}
}
