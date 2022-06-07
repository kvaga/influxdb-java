package test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SendTask implements Runnable{
	private static Logger log = LogManager.getLogger(SendTask.class);
	private String metric=null, value=null, host=null, dbName=null;
	private int port=0;
	private int COUNT_OF_ATTEMPTS=5;
	private long CONNECTION_TIMEOUT_MS=5000;
	
	public SendTask(String metric, String value, String host, int port, String dbName, int countOfAttempts, long connectionTimeoutInMillis) {
		this.metric=metric;
		this.value=value;
		this.host=host;
		this.port=port;
		this.dbName=dbName;
		COUNT_OF_ATTEMPTS=countOfAttempts;
		CONNECTION_TIMEOUT_MS=connectionTimeoutInMillis;
	}
	
	
	public SendTask(String metric, int value, String host, int port, String dbName, int countOfAttemptsIfFails, long timeout) {
		this(metric, String.valueOf(value), host, port, dbName, countOfAttemptsIfFails, timeout);
	}
	
	public SendTask(String metric, long value, String host, int port, String dbName, int countOfAttemptsIfFails, long timeout) {
		this(metric, String.valueOf(value), host, port, dbName, countOfAttemptsIfFails, timeout);
	}
	public void run() {
		String data = metric + " value=" + value;
		HttpClient httpclient = HttpClients.createDefault();
		URI uri;
		HttpPost httppost;
		StringEntity entity;
		try {
			
			uri = new URIBuilder().setScheme("http") // Can be https if ssl is enabled
					.setHost(host).setPort(port).setPath("/write").setParameter("db", dbName)
					// .setParameter("u", "username") // The username of the account we created
					// .setParameter("p", "password") // The password of the account
					.build();
			httppost = new HttpPost(uri);
			
			entity = new StringEntity(data, ContentType.create("plain/text"));
			httppost.setEntity(entity);
			Timeout timeout = Timeout.ofMilliseconds(CONNECTION_TIMEOUT_MS);
			RequestConfig requestConfig = RequestConfig.custom()
				    .setConnectionRequestTimeout(timeout)
				    .setResponseTimeout(timeout)
				    .setConnectTimeout(timeout)
				    .build();
			httppost.setConfig(requestConfig);

			for( int i=0; i<COUNT_OF_ATTEMPTS-1;i++) {
				if(send(httpclient, httppost)==204) {
					//System.err.println("OK");
					return;
				}
			}
			log.error("Couldn't send data to influxdb after ["+COUNT_OF_ATTEMPTS+"] attemtps. Parameters: " + this);
		} catch (URISyntaxException e) {
			log.error("URISyntaxException", e);
		} catch (IOException e) {
			log.error("IOException", e);
		}catch(Exception e) {
			log.error("Exception", e);
		}
	}
	private int send(HttpClient httpclient, HttpPost httppost) throws IOException {
		org.apache.hc.core5.http.HttpResponse httpResponse = httpclient.execute(httppost);
		return httpResponse.getCode();
	}

//	public String toString() {
//		StringBuilder sb = new StringBuilder();
//		for(Field field : getClass().getDeclaredFields()) {
//			if(field.getModifiers()!=(Modifier.STATIC + Modifier.PRIVATE)) {
////				field.setAccessible(true);
//				sb.append(field.getName()); sb.append(": "); 
//				try {
//					sb.append(field.get(this));
//				} catch (IllegalArgumentException e) {
//					log.error("IllegalArgumentException", e);
//				} catch (IllegalAccessException e) {
//					log.error("IllegalAccessException", e);
//				} 
//				sb.append(" | ");
//			}
//		}
//		return sb.toString();
//	}
	
}
