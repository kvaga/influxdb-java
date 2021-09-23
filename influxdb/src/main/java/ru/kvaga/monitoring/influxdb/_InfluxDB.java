package ru.kvaga.monitoring.influxdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class _InfluxDB {
	private static Logger log = LogManager.getLogger(_InfluxDB.class);
	// TODO:
	// https://www.baeldung.com/java-influxdb
	private static _InfluxDB influxdb = null;
	private static boolean ENABLED = false;
	private int THREADS_NUMBER=10;
	private static ExecutorService exec;
	private static int _countOfAttemptsIfFails=5;
	private static long _timeout=5000;
	
	private String host;
	private int port;
	private String dbName;

	private _InfluxDB(String host, int port, String dbName, int threadsNumber) {
		this.host = host;
		this.port = port;
		this.dbName = dbName;
		this.THREADS_NUMBER=threadsNumber;
		exec = Executors.newFixedThreadPool(THREADS_NUMBER);
	}

	public static _InfluxDB getInstance(String host, int port, String dbName, int threadsNumber) {
		if (influxdb == null) {
			influxdb = new _InfluxDB(host, port, dbName, threadsNumber);
		}
		return influxdb;
	}
	
	
	public static _InfluxDB getInstance() {
		return influxdb;
	}
	

	/**
	 * return 204 OK
	 * 
	 * @param metric
	 * @param value
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public void send(String metric, String value)  {
		if (!ENABLED)
			return; 
		exec.execute(new SendTask(metric, value, host, port, dbName, _countOfAttemptsIfFails, _timeout));
	}
	public void send(String metric, int value)  {
		if (!ENABLED)
			return;
		exec.execute(new SendTask(metric, value, host, port, dbName, _countOfAttemptsIfFails, _timeout));
	}
	public void send(String metric, long value)  {
		if (!ENABLED)
			return;
		exec.execute(new SendTask(metric, value, host, port, dbName, _countOfAttemptsIfFails, _timeout));
	}

	/**
	 * return 200 OK
	 * 
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public int createDatabase() throws URISyntaxException, IOException {
		if (!ENABLED) return -1;
		String data = "";
		HttpClient httpclient = HttpClients.createDefault();
		URI uri = new URIBuilder().setScheme("http") // Can be https if ssl is enabled
				.setHost(host).setPort(port).setPath("/query").setParameter("q", "CREATE DATABASE " + dbName)
				// .setParameter("u", "username") // The username of the account we created
				// .setParameter("p", "password") // The password of the account
				.build();
		HttpPost httppost = new HttpPost(uri);
		StringEntity entity = new StringEntity(data, ContentType.create("plain/text"));
		httppost.setEntity(entity);
		org.apache.hc.core5.http.HttpResponse httpResponse = httpclient.execute(httppost);
		if(httpResponse.getCode()==200) {
			log.debug("Database created ");
		}
		return httpResponse.getCode();
	}

	/**
	 * return 200 OK
	 * 
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public int deleteDatabase() throws URISyntaxException, IOException {
		String data = "";
		HttpClient httpclient = HttpClients.createDefault();
		URI uri = new URIBuilder().setScheme("http") // Can be https if ssl is enabled
				.setHost(host).setPort(port).setPath("/query").setParameter("q", "DROP DATABASE " + dbName)
				// .setParameter("u", "username") // The username of the account we created
				// .setParameter("p", "password") // The password of the account
				.build();
		HttpPost httppost = new HttpPost(uri);
		StringEntity entity = new StringEntity(data, ContentType.create("plain/text"));
		httppost.setEntity(entity);
		org.apache.hc.core5.http.HttpResponse httpResponse = httpclient.execute(httppost);
		return httpResponse.getCode();
	}
	
	public static void destroy() {
		exec.shutdownNow();
	}
	
	public static void enable() {
		ENABLED=true;
	}
	public static void disable() {
		ENABLED=false;
	}
	
	public static void setTimeoutInMillis(long timeout) {
		_timeout=timeout;
	}
	public static void setCountOfAttemptsIfFails(int countOfAttemptsIfFails) {
		_countOfAttemptsIfFails=countOfAttemptsIfFails;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("InfluxDB parameters [");
		sb.append("ENABLED");sb.append("=");sb.append(ENABLED);sb.append(", ");
		sb.append("THREADS_NUMBER");sb.append("=");sb.append(THREADS_NUMBER);sb.append(", ");
		sb.append("countOfAttemptsIfFails");sb.append("=");sb.append(_countOfAttemptsIfFails);sb.append(", ");
		sb.append("timeout");sb.append("=");sb.append(_timeout);sb.append(", ");
		sb.append("host");sb.append("=");sb.append(host);sb.append(", ");
		sb.append("port");sb.append("=");sb.append(port);sb.append(", ");
		sb.append("dbName");sb.append("=");sb.append(dbName);sb.append("");
		sb.append("]");

		return sb.toString();
	}
}
