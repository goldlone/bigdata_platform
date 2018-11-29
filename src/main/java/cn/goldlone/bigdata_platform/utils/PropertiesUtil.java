package cn.goldlone.bigdata_platform.utils;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {
	
	private String fileName;
	private Properties properties = new Properties();
	
	public PropertiesUtil(String fileName) {
		this.fileName = fileName;
		open();
	}
	
	private void open() {
		try {
			properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据key取出properties文件中对应的value
	 * @param key
	 * @return value
	 */
	public String readPropertyByKey(String key) {
		return properties.getProperty(key);
	}

	/**
	 * 从文件中读取配置后将整个的Properties返回
	 * @return properties成员
	 */
	public Properties getProperties(){
		return this.properties;
	}

}
