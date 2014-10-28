package com.radiadesign.catalina.session;

import org.apache.catalina.session.ManagerBase;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public abstract class AbstractRedisSessionManager extends ManagerBase {
	protected byte[] NULL_SESSION = "null".getBytes();

	protected String host = "localhost";
	protected int port = 6379;
	protected int database = 0;
	protected String password = null;
	protected int timeout = Protocol.DEFAULT_TIMEOUT;
	
	protected boolean testOnBorrow; 
	protected boolean testWhileIdle;
	protected boolean testOnReturn;
	
	protected int maxWait; 
	protected int maxIdle;
	protected int minIdle;
	
	/** 
	 * Properties that should be ignored when comparing for dirty reference.
	 */
	protected String ignoredProperties;
	
	protected String serializationStrategyClass = "com.radiadesign.catalina.session.JavaSerializer";
	
	public void setSerializationStrategyClass(String strategy) {
		this.serializationStrategyClass = strategy;
	}
	
	public JedisPoolConfig getJedisConfig() {
		JedisPoolConfig config = new JedisPoolConfig();
		
		config.setTestOnBorrow(testOnBorrow);
		config.setTestOnReturn(testOnReturn);
		config.setTestWhileIdle(testWhileIdle);
		
		
		config.setMaxTotal(maxActive);
		config.setMaxWaitMillis(maxWait);
		config.setMaxIdle(maxIdle);
		config.setMinIdle(minIdle);
		
		return config;
	}
	
	public int getMaxWait() {
		return maxWait;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}
	
	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public String getSerializationStrategyClass() {
		return serializationStrategyClass;
	}

	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getDatabase() {
		return database;
	}
	
	public void setDatabase(int database) {
		this.database = database;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public int getTimeout() {
		return timeout;
	}
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getIgnoredProperties() {
		return ignoredProperties;
	}

	public void setIgnoredProperties(String ignoredProperties) {
		this.ignoredProperties = ignoredProperties;
	}
}
