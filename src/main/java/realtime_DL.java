import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders; // Import Encoders chuẩn Spark 3.5
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class realtime_DL {
    private static final String ONNX_MODEL_PATH = "/home/doringu123/SparkKafkaConsumer/models/deep_mlp/deep_mlp_model.onnx";
    private static final String SCALER_MEAN_PATH = "/home/doringu123/SparkKafkaConsumer/models/deep_mlp/scaler_mean.txt";
    private static final String SCALER_STD_PATH = "/home/doringu123/SparkKafkaConsumer/models/deep_mlp/scaler_std.txt";
    
    private static final String CHECKPOINT_PATH = "/home/doringu123/SparkKafkaConsumer/checkpoints/realtime_dl";
    private static final String METRICS_CSV = "/home/doringu123/SparkKafkaConsumer/metrics/stream_metrics.csv";
    
    private static final double TRIGGER_INTERVAL_SEC = 5; 

    private static double[] loadScalerParams(String filePath) throws Exception {
        List<Double> list = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(filePath))) {
            while (scanner.hasNextDouble()) {
                list.add(scanner.nextDouble());
            }
        }
        return list.stream().mapToDouble(Double::doubleValue).toArray();
    }

    private static void logMetricsToCSV(long batchId, long totalRows, long normalRows, long anomalyRows, long latencyMs, double inputRowsPerSecond, double processedRowsPerSecond) {
        try {
            File file = new File(METRICS_CSV);
            boolean fileExists = file.exists();
            FileWriter fw = new FileWriter(file, true);
            PrintWriter pw = new PrintWriter(fw);

            if (!fileExists || file.length() == 0) {
                pw.println("timestamp,batchId,totalRows,normalRows,anomalyRows,latencyMs,inputRowsPerSecond,processedRowsPerSecond");
            }

            pw.println(LocalDateTime.now() + "," + batchId + "," + totalRows + "," + normalRows + "," + anomalyRows + "," + latencyMs + "," + String.format("%.2f", inputRowsPerSecond) + "," + String.format("%.2f", processedRowsPerSecond));
            pw.flush();
            pw.close();
        } catch (Exception e) {
            System.err.println("CSV LOGGER ERROR: " + e.getMessage());
        }
    }

    public static void myCustomFunc(Dataset<Row> df, long batchID) {
        df.persist(StorageLevel.MEMORY_AND_DISK());
        long startTime = System.currentTimeMillis();

        try {
            Dataset<Row> normalDF = df.filter(col("prediction").equalTo(0.0));
            normalDF.withColumn("value", to_json(struct("*")))
                    .selectExpr("CAST(value AS STRING)").write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "normalRealtime2")
                    .save();

            Dataset<Row> anomalyDF = df.filter(col("prediction").equalTo(1.0));
            anomalyDF.withColumn("value", to_json(struct("*")))
                    .selectExpr("CAST(value AS STRING)").write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "anormalRealtime2")
                    .save();

            long totalRows = df.count();
            long normalRows = normalDF.count();
            long anomalyRows = anomalyDF.count();
            
            long endTime = System.currentTimeMillis();
            long latencyMs = endTime - startTime;

            double inputRowsPerSecond = totalRows / TRIGGER_INTERVAL_SEC;
            double processedRowsPerSecond = (latencyMs > 0) ? (totalRows * 1000.0 / latencyMs) : 0.0;

            logMetricsToCSV(batchID, totalRows, normalRows, anomalyRows, latencyMs, inputRowsPerSecond, processedRowsPerSecond);

            System.out.println("\n>>> BATCH: " + batchID + " | Total: " + totalRows + " | Anomaly: " + anomalyRows);
            System.out.println(">>> Latency: " + latencyMs + "ms | ProcessRate: " + String.format("%.2f", processedRowsPerSecond) + " rows/s");

        } catch (Exception e) {
            System.err.println("BATCH ERROR: " + e.getMessage());
        } finally {
            df.unpersist();
        }
    }

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        SparkSession spark = SparkSession.builder()
                .appName("WaterLog-DeepMLP-Realtime")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "4") 
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "512m")
                .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
                .getOrCreate();

        StructType schema = new StructType()
                .add("Treat_ScenarioID", DataTypes.DoubleType)
                .add("Treat_Level2", DataTypes.DoubleType)
                .add("Treat_Level1", DataTypes.DoubleType)
                .add("Treat_CurrentVolume", DataTypes.DoubleType)
                .add("Dam_Level", DataTypes.DoubleType)
                .add("Dam_CurrentVolume", DataTypes.DoubleType)
                .add("Treat_PumpFlowLiterMin", DataTypes.DoubleType)
                .add("Treat_Pump3", DataTypes.DoubleType)
                .add("Treat_Pump2", DataTypes.DoubleType)
                .add("Treat_Pump1", DataTypes.DoubleType)
                .add("Treat_LimitSwitch1", DataTypes.DoubleType)
                .add("Dam_Pump3", DataTypes.DoubleType)
                .add("Dam_Pump2", DataTypes.DoubleType)
                .add("Dam_Pump1", DataTypes.DoubleType)
                .add("Dam_LimitSwitch", DataTypes.DoubleType)
                .add("Dam_Chlor_Raw", DataTypes.DoubleType)
                .add("label", DataTypes.DoubleType);

        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "jsonproducerp3")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 500000)
                .load();

        Dataset<Row> parsedDF = kafkaDF.selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).as("data"))
                .select("data.*");

        StructType newSchema = parsedDF.schema().add("prediction", DataTypes.DoubleType);
        String[] featureCols = parsedDF.drop("label").columns();

        Dataset<Row> predictionDF = parsedDF.mapPartitions((Iterator<Row> iterator) -> {
            
            OrtEnvironment env = OrtEnvironment.getEnvironment();
            OrtSession session = env.createSession(ONNX_MODEL_PATH, new OrtSession.SessionOptions());
            
            double[] means = loadScalerParams(SCALER_MEAN_PATH);
            double[] stds = loadScalerParams(SCALER_STD_PATH); 
            
            List<Row> outputRows = new ArrayList<>();
            
            while (iterator.hasNext()) {
                Row row = iterator.next();
                float[] features = new float[featureCols.length]; 
                
                for (int i = 0; i < featureCols.length; i++) {
                    Object val = row.getAs(featureCols[i]);
                    double rawVal = (val != null) ? ((Number) val).doubleValue() : 0.0;
                    features[i] = (float) ((rawVal - means[i]) / (stds[i] + 1e-7));
                }
                
                float[][] inputMatrix = new float[][]{features};
                OnnxTensor inputTensor = OnnxTensor.createTensor(env, inputMatrix);
                
                try (OrtSession.Result results = session.run(Collections.singletonMap("float_input", inputTensor))) {
                    float[][] outputProbs = (float[][]) results.get(0).getValue();
                    double prediction = outputProbs[0][0] >= 0.5 ? 1.0 : 0.0;
                    
                    Object[] rowValues = new Object[row.length() + 1];
                    for (int k = 0; k < row.length(); k++) {
                        rowValues[k] = row.get(k);
                    }
                    rowValues[row.length()] = prediction;
                    
                    outputRows.add(RowFactory.create(rowValues));
                }
                inputTensor.close();
            }
            session.close();
            return outputRows.iterator();
        }, Encoders.row(newSchema)); // Đã sửa đổi sử dụng API Encoders.row chuẩn Spark 3.5

        StreamingQuery query = predictionDF.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) realtime_DL::myCustomFunc)
                .option("checkpointLocation", CHECKPOINT_PATH)
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("DEEP MLP REALTIME MONITORING ENABLED. LOGGING TO: " + METRICS_CSV);
        query.awaitTermination();
    }
}
