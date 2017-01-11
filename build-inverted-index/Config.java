package com.neu.ir.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

	private Properties properties;
	

	private Config(){}

	// read the configuration into properties object
	public  static Config getInstance(String configFile){		

	    Config newConfig = new Config();
		newConfig.properties = new Properties();
	    try {
	        properties.load(new FileInputStream(configFile));
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
 	}

	public String getCollectionDirPath()
	{
		return properties.getProperty("collectionPath");
	}
	
	public String getQueryFilePath()
	{
		return properties.getProperty("queryFile");
	}
	
	public String getStopListPath()
	{
		return properties.getProperty("stopWordsFile");
	}
	
	public String getIdDocNoPath()
	{
		return properties.getProperty("idDocNoFile");
	}
	
	public String getQrelFile()
	{
		return properties.getProperty("qrelFile");
	}
}
