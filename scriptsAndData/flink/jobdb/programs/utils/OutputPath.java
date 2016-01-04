package de.hshannover.vis.flink.jobdb.utils;

public class OutputPath implements IParameter{

	
	private String name,path;
	
	public OutputPath(String name, String path) {
		super();
		this.name = name;
		this.path = path;
	}
	
	public String getParamName() {
		return name;
	}
	
	public void setPath(String path){
		this.path = path;
	}

	public String getPath() {
		return path;
	}
	
}
