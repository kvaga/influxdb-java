package ru.kvaga.monitoring.influxdb;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

/**
 * InfluxDB version 2
 * @author kvaga
 *
 */
public class InfluxDB2 {
	private static Logger log = LogManager.getLogger(InfluxDB2.class);
	
    private String token;
    private String orgId;
    private String bucket;
    private String url;
    private static InfluxDBClient influxDBClient=null;
    private static InfluxDB2 thisInstance=null;
    private static WriteApiBlocking writeApi=null;
    private static QueryApi queryApi = null;

    public String toString() {
		return "InfluxDB2 instance parameters [bucket="+bucket+", orgId="+orgId+", url="+url+", token=token]";
	}
    
    public static InfluxDB2 getInstance() {
    	return thisInstance;
    }
    public static InfluxDB2 getInstance(String url, String orgId, String bucket, String token) {
		if (thisInstance == null) {
			thisInstance = new InfluxDB2(url, orgId, bucket, token);
		}
		return thisInstance;
	}
   
    public InfluxDB2(String url, String orgId, String bucket, String token) {
    	this.url=url;
    	this.orgId=orgId;
    	this.bucket=bucket;
    	this.token=token;
        influxDBClient = InfluxDBClientFactory.create(this.url, this.token.toCharArray(), this.orgId, this.bucket);
        writeApi = influxDBClient.getWriteApiBlocking();
        queryApi = influxDBClient.getQueryApi();
    }
    
    /**
     * Write by Data Point
     * @param point (com.influxdb.client.write.Point)
     * Example: Point point = Point.measurement("temperature")
     *            .addTag("location", "west")
     *            .addField("value", 55D)
     *            .time(Instant.now().toEpochMilli(), WritePrecision.MS);
     *  @param writePrecision (com.influxdb.client.domain.WritePrecision)
     *  Example: WritePrecision.NS
     */
    public void send(Point point, WritePrecision writePrecision) {
         writeApi.writePoint(point);
	}
    
    /**
     * Write by LineProtocol
     * @param line (java.lang.String)
     * Example: "temperature,location=north value=60.0"
     * @param writePrecision (com.influxdb.client.domain.WritePrecision)
     * Example: WritePrecision.NS
     */
    public void send(String line, WritePrecision writePrecision) {   
        //
        // Write by LineProtocol
        //
        writeApi.writeRecord(WritePrecision.NS, "temperature,location=north value=60.0");
	}
    
    /**
     * Write by LineProtocol
     * @param metric (java.lang.String)
     * Example: "temperature"
     * @param value (java.lang.String)
     * Example: "60.0"
     * @param writePrecision (com.influxdb.client.domain.WritePrecision)
     * Example: WritePrecision.NS
     */
    public void send(String metric, String value, WritePrecision writePrecision) {   
        //
        // Write by LineProtocol
        //
        writeApi.writeRecord(WritePrecision.NS, metric+" value="+value);
	}
    
    /**
     * 
     * @param pojo
     * Example: 
     *  @Measurement(name = "temperature")
     *  private static class Temperature {
     *  	@Column(tag = true)
     *  	String location;
     *  	@Column
     *  	Double value;
     *  	@Column(timestamp = true)
     *  	Instant time;
     *  }
     *  Temperature temperature = new Temperature();
     *  temperature.location = "south";
     *  temperature.value = 62D;
     *  temperature.time = Instant.now();
     *  writeApi.writeMeasurement( WritePrecision.NS, temperature);
     * @param writePrecision (com.influxdb.client.domain.WritePrecision)
     * Example: WritePrecision.NS
     */
    public void sendPOJO(Object pojo, WritePrecision writePrecision) {
        writeApi.writeMeasurement( WritePrecision.NS, pojo);
    }

    /**
     * 
     * @param fluxQuery (java.lang.String)
     * Example: String fluxQuery = "from(bucket:\""+bucket+"\") |> range(start: 0)";
     * @return tables (List<FluxTable>)
     * Example:
     * List<FluxTable> tables = queryData(fluxQuery);
     * for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
            }
        }
     */
    public List<FluxTable> queryData(String fluxQuery){
        return queryApi.query(fluxQuery);
    }
       
    public void destroy() {
    	try {
    		influxDBClient.close();
    	}catch(Exception e) {
    		
    	}
	}
	
	
}
