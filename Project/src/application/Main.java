package application;
	
import java.awt.Container;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.io.Files;

import javafx.application.Application;
import javafx.stage.Stage;
import scala.Tuple2;
import scala.Tuple6;
import streaming.EventServer;
import streaming.SparkConfiguration;
import utils.Constants;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;


public class Main {
	
	private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);
//	@Override
//	public void start(Stage primaryStage) {
//		try {
//			BorderPane root = new BorderPane();
//			Scene scene = new Scene(root,400,400);
//			scene.getStylesheets().add(getClass().getResource("application.css").toExternalForm());
//			primaryStage.setScene(scene);
//			primaryStage.show();
//		} catch(Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	
	public static void main(String[] args) {
		
		final Interface inter = new Interface();
		
		new Thread() {
            @Override
            public void run() {
            	 javafx.application.Application.launch(Interface.class);
            }
        }.start();
		
        final File outputFile = new File(Constants.DATABASE_FILE);

        SparkConf sparkConf = new SparkConf()
        		  .setAppName("App")
        		  .setMaster("local[8]");
        
        final JavaStreamingContext streamingContext =
                new JavaStreamingContext(sparkConf, Durations.seconds(3));
        streamingContext.checkpoint(Constants.TEMP_FILE);


        Logger.getRootLogger().setLevel(Level.ERROR);
        //final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(streamingContext.sparkContext());
        
        
        JavaPairRDD<String, String> fileNameContentsRDD = streamingContext.sparkContext().wholeTextFiles(Constants.DATA_FILE);
        
        JavaRDD<String> files = fileNameContentsRDD.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;
			public String call(Tuple2<String, String> fileNameContent) {
                return fileNameContent._2();
            }
        });
        
        
       BlockingQueue<String> fileEvents = new LinkedBlockingQueue<String>(files.collect());
       EventServer eventServer = new EventServer(fileEvents);
       eventServer.sendEvents();
        
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 10001);
        ;
        
        JavaPairDStream<String, Sensor> pairs = lines.mapToPair(new PairFunction<String, String, Sensor>() {
        	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Sensor> call(String t) {
				
				try {
					JSONObject tempJson = new JSONObject(t);
	                JSONObject recordJson = tempJson.getJSONObject("record");
	                JSONArray sdataJson = recordJson.getJSONArray("sdata");
	                JSONArray sensorsJson = ((JSONObject)sdataJson.get(0)).getJSONArray("sensors");
	                for(int i = 0;i < sensorsJson.length();i++) { 
	                	JSONObject json = sensorsJson.getJSONObject(i);
	                	if(json.get(Constants.SENSOR_TYPE).equals("Ext_Tem")) {
	                		runningMax.set(Math.max(runningMax.get(), json.getInt(Constants.VALUE)));
	                		System.out.println("Max: " + runningMax.get());
	                		
	                		
	                		Sensor sensor = new Sensor(json.getString(Constants.SENSOR_TYPE), json.getInt(Constants.VALUE));
	                		return new Tuple2<String, Sensor>("sensor", sensor);
	                		
	                		
	                	}
	                }
	        		return null;
				} catch(Exception e) {
					return null;
				}
        		
        	}
        });
        
        pairs.foreachRDD(new VoidFunction2<JavaPairRDD<String, Sensor>, Time>() {
        	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

        	public void call(final JavaPairRDD<String, Sensor> rdd, Time time) {
        		
        		try {
        			Sensor sensor = rdd.first()._2();
            		System.out.println("Value of sensor is " + sensor.value);
            		//Files.append(rdd.first()._1() +"," + rdd.first()._2() + "\n", outputFile, Charset.defaultCharset());
        		} catch(Exception e) {
        			
        		}
        		
        		
        	}
        });
        
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.close();
				
	}
}
