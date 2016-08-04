package streaming;

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
import scala.Tuple2;
import utils.Constants;

public class SparkConfiguration {
	
	private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);
	
	public void start() {
		
		String dataFile = "/home/cristina/Desktop/Eclipse/license/directory/";
        String tempFile = "/home/cristina/Desktop/Eclipse/license/tempFile/";
        final String dbFile = "/home/cristina/Desktop/Eclipse/license/dbFile.csv";
        final File outputFile = new File(dbFile);

        SparkConf sparkConf = new SparkConf()
        		  .setAppName("App")
        		  .setMaster("local[8]");
        
        final JavaStreamingContext streamingContext =
                new JavaStreamingContext(sparkConf, Durations.seconds(3));
        streamingContext.checkpoint(tempFile);


        Logger.getRootLogger().setLevel(Level.ERROR);
        //final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(streamingContext.sparkContext());
        
        
        JavaPairRDD<String, String> fileNameContentsRDD = streamingContext.sparkContext().wholeTextFiles(dataFile);
        
        JavaRDD<String> files = fileNameContentsRDD.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;
			public String call(Tuple2<String, String> fileNameContent) throws Exception {
                return fileNameContent._2();
            }
        });
        
        
       BlockingQueue<String> fileEvents = new LinkedBlockingQueue<String>(files.collect());
       EventServer eventServer = new EventServer(fileEvents);
       eventServer.sendEvents();
        
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 10001);
        ;
        
        JavaPairDStream<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
        	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String t) throws Exception {
        		JSONObject tempJson = new JSONObject(t);
                JSONObject recordJson = tempJson.getJSONObject("record");
                JSONArray sdataJson = recordJson.getJSONArray("sdata");
                JSONArray sensorsJson = ((JSONObject)sdataJson.get(0)).getJSONArray("sensors");
                for(int i = 0;i < sensorsJson.length();i++) { 
                	JSONObject json = sensorsJson.getJSONObject(i);
                	if(json.get(Constants.SENSOR_TYPE).equals("Ext_Tem")) {
                		runningMax.set(Math.max(runningMax.get(), json.getInt(Constants.VALUE)));
                		System.out.println("Max: " + runningMax.get());
                		
                		return new Tuple2<String, Integer>(json.getString(Constants.SENSOR_TYPE), 
                				json.getInt(Constants.VALUE));
                	}
                }
        		return null;
        	}
        });
        
        pairs.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
        	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(final JavaPairRDD<String, Integer> rdd, Time time) throws IOException {
        		Files.append(rdd.first()._1() +"," + rdd.first()._2() + "\n", outputFile, Charset.defaultCharset());
        	}
        });
        
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.close();
	}
}
