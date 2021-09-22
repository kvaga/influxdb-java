package ru.kvaga.monitoring.influxdb2;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;


public class InfluxDB2 {
	private static Logger log = LogManager.getLogger(InfluxDB2.class);

	private static InfluxDB influxDB = null;
	private static InfluxDB2 thisInstance;
	private String influxdbDatabaseName;
	private String influxdbHost;
	private int	influxdbPort=8086;
	private String retentionPolicyName="defaultPolicy";
	private String retentionPolicyDuration="7d";
	private String userName="q";
	private String userPassword="w";
	
	private InfluxDB2(String influxdbHost, int influxdbPort, String influxdbDatabaseName, String userName, String userPassword, String retentionPolicyName, String retentionPolicyDuration) {
		this.influxdbHost = influxdbHost;
		this.influxdbPort = influxdbPort;
		this.influxdbDatabaseName = influxdbDatabaseName;
		if(retentionPolicyName!=null) {
			log.debug("Default value of retentionPolicyName ["+this.retentionPolicyName+"] changed to the ["+retentionPolicyName+"]");
			this.retentionPolicyName=retentionPolicyName;
		}
		if(retentionPolicyDuration!=null) {
			log.debug("Default value of retentionPolicyDuration ["+this.retentionPolicyDuration+"] changed to the ["+retentionPolicyDuration+"]");
			this.retentionPolicyDuration=retentionPolicyDuration;
		}
		if(userName!=null) {
			log.debug("Default value of userName ["+this.userName+"] changed to the ["+userName+"]");
			this.userName=userName;
		}
		if(userPassword!=null) {
			log.debug("Default value of userPassword ["+this.userPassword+"] changed");
			this.userPassword=userPassword;
		}
		
		String databaseURL=String.format("http://%s:%d?db=%s", influxdbHost, influxdbPort, influxdbDatabaseName);
		influxDB = InfluxDBFactory.connect(databaseURL, this.userName, this.userPassword);
		influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
		influxDB.enableBatch(100, 1000, TimeUnit.MILLISECONDS);
		createDB();
		influxDB.setRetentionPolicy(this.retentionPolicyName);
		influxDB.setDatabase(this.influxdbDatabaseName);
		
		if(ping()) {
			log.debug("Ping ["+databaseURL+"] success. " + this);
		}else {
			log.error("Ping ["+databaseURL+"] failed" + this);
		}
	}

	public static InfluxDB2 getInstance(String influxdbHost, int influxdbPort, String influxdbDatabaseName) {
		if (thisInstance == null) {
			thisInstance = new InfluxDB2(influxdbHost, influxdbPort, influxdbDatabaseName, null, null, null, null);
		}
		return thisInstance;
	}
	
	public static InfluxDB2 getInstance(String influxdbUserName, String influxdbUserPassword, String influxdbHost, int influxdbPort, String influxdbDatabaseName) {
		if (thisInstance == null) {
			thisInstance = new InfluxDB2(influxdbHost, influxdbPort, influxdbDatabaseName, influxdbUserName, influxdbUserPassword, null, null);
		}
		return thisInstance;
	}
	
	public static InfluxDB2 getInstance(String influxdbUserName, String influxdbUserPassword, String influxdbHost, int influxdbPort, String influxdbDatabaseName, String retentionPolicyName, String retentionPolicyDuration) {
		if (thisInstance == null) {
			thisInstance = new InfluxDB2(influxdbHost, influxdbPort, influxdbDatabaseName, influxdbUserName, influxdbUserPassword, retentionPolicyName, retentionPolicyDuration);
		}
		return thisInstance;
	}
	
	
	public void send(String metric, Integer value) {
		Point point = Point.measurement(metric)
				  .time(System.currentTimeMillis() - 100, TimeUnit.MILLISECONDS)
				  .addField("v", value)
//				  .addField("free", 4743696L)
//				  .addField("used", 1016096L)
//				  .addField("buffer", 1008467L)
				  .build();
		send(point);
	}
	public void send(Point point) {
		influxDB.write(point);
	}
	public static boolean ping() {
		Pong response = influxDB.ping();
		if (response.getVersion().equalsIgnoreCase("unknown")) {
		    System.err.println("Error pinging server.");
		    return false;
		} else {
			return true;
		}
	}
	
	public void createDB() {
		if(!influxDB.databaseExists(influxdbDatabaseName))
			influxDB.createDatabase(influxdbDatabaseName);
		
		influxDB.createRetentionPolicy(
		  retentionPolicyName, influxdbDatabaseName, retentionPolicyDuration, 1, true);
	}
	
	public  void destroy() {
		influxDB.close();
	}
	
	public String toString() {
		return "InfluxDB2 instance parameters [influxdbDatabaseName="+influxdbDatabaseName+", influxdbHost="+influxdbHost+", influxdbPort="+influxdbPort+", retentionPolicyName="+retentionPolicyName+", retentionPolicyDuration="+retentionPolicyDuration+", userName="+userName+"]";
	}
	
}
