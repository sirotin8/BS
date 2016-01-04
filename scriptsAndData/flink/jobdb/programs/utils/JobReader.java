package de.hshannover.vis.flink.jobdb.utils;

public class JobReader {
	
	public static String getString(String name) {
		String[] arr = name.split("\"");
		String result = "";
		if(arr.length>=2)
			result = arr[1];
		return result;
	}
	
	public static String getDate(String name){
		
		String result = name;
		
		return result;
		
	}

	public static Integer getInteger(String name) {
		name = name.replaceAll(" ", "");
		if (name.equals(""))
			return new Integer(-1);
		return Integer.parseInt(name);
	}

	public static Double getDouble(String name) {
		name = name.replaceAll(" ", "");
		name = name.replaceAll(",", ".");
		if (name.equals(""))
			return new Double(-1.0);
		return Double.parseDouble(name);
	}

	public static String[] prepareString(String line){

		line += " ";
		String[] result = line.split("\t");	
				
		return result;
	}

}
