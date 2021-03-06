package me.dekimpe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class App 
{
    public static void main( String[] args )
    {
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parsing XML - Session")
                .master("spark://192.168.10.14:7077")
                .getOrCreate();
        
        Dataset<Row> df = spark.read()
                .format("com.databricks.spark.xml")
                .option("rootTag", "mediawiki")
                .option("rowTag", "page")
                .load("hdfs://hdfs-namenode:9000/input/" + args[0]);
        
        df.write().mode(SaveMode.Overwrite).format("avro").save("hdfs://hdfs-namenode:9000/schemas/" + args[1]);
        
    }
}
