import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.LocalDateTime;

import static org.apache.spark.sql.functions.*;

public class realtime_ML {
    private static final String MODEL_PATH = "/home/doringu123/SparkKafkaConsumer/models/ML/randomforest";
    private static final String CHECKPOINT_PATH = "/home/doringu123/SparkKafkaConsumer/checkpoints/realtime_v1";
    private static final String METRICS_CSV = "/home/doringu123/SparkKafkaConsumer/metrics/stream_metrics.csv";
    
    // Chu kỳ trigger (giây) - dùng để tính InputRowsPerSecond
    private static final double TRIGGER_INTERVAL_SEC = 5;

    private static void logMetricsToCSV(long batchId, long totalRows, long normalRows, long anomalyRows, long latencyMs, double inputRowsPerSecond, double processedRowsPerSecond) {
        try {
            File file = new File(METRICS_CSV);
            boolean fileExists = file.exists();
            FileWriter fw = new FileWriter(file, true);
            PrintWriter pw = new PrintWriter(fw);

            if (!fileExists || file.length() == 0) {
                pw.println("timestamp,batchId,totalRows,normalRows,anomalyRows,latencyMs,inputRowsPerSecond,processedRowsPerSecond");
            }

            pw.println(LocalDateTime.now() + "," + 
                       batchId + "," + 
                       totalRows + "," + 
                       normalRows + "," + 
                       anomalyRows + "," + 
                       latencyMs + "," + 
                       String.format("%.2f", inputRowsPerSecond) + "," + 
                       String.format("%.2f", processedRowsPerSecond));
            pw.flush();
            pw.close();
        } catch (Exception e) {
            System.err.println("CSV LOGGER ERROR: " + e.getMessage());
        }
    }

    public static void myCustomFunc(Dataset<Row> df, long batchID) {
        // Tối ưu hóa: Lưu tạm dữ liệu vào RAM
        df.persist(StorageLevel.MEMORY_AND_DISK());
        long startTime = System.currentTimeMillis();

        try {
            // 1. Phân loại và gửi Kafka
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

            // 2. Thu thập chỉ số (Metrics)
            long totalRows = df.count();
            long normalRows = normalDF.count();
            long anomalyRows = anomalyDF.count();
            
            long endTime = System.currentTimeMillis();
            long latencyMs = endTime - startTime;

            // Tính toán tốc độ
            double inputRowsPerSecond = totalRows / TRIGGER_INTERVAL_SEC;
            double processedRowsPerSecond = (latencyMs > 0) ? (totalRows * 1000.0 / latencyMs) : 0.0;

            // 3. Ghi vào file CSV
            logMetricsToCSV(batchID, totalRows, normalRows, anomalyRows, latencyMs, inputRowsPerSecond, processedRowsPerSecond);

            // 4. In Console
            System.out.println("\n>>> BATCH: " + batchID + " | Total: " + totalRows + " | Anomaly: " + anomalyRows);
            System.out.println(">>> Latency: " + latencyMs + "ms | ProcessRate: " + String.format("%.2f", processedRowsPerSecond) + " rows/s");

        } catch (Exception e) {
            System.err.println("BATCH ERROR: " + e.getMessage());
        } finally {
            df.unpersist(); // Giải phóng bộ nhớ
        }
    }

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        SparkSession spark = SparkSession.builder()
                .appName("WaterLog-Realtime-Research")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "512m")
                .getOrCreate();

        StructType schema = new StructType()
                .add("Treat_ScenarioID", DataTypes.DoubleType).add("Treat_Level2", DataTypes.DoubleType)
                .add("Treat_Level1", DataTypes.DoubleType).add("Treat_CurrentVolume", DataTypes.DoubleType)
                .add("Dam_Level", DataTypes.DoubleType).add("Dam_CurrentVolume", DataTypes.DoubleType)
                .add("Treat_PumpFlowLiterMin", DataTypes.DoubleType).add("Treat_Pump3", DataTypes.DoubleType)
                .add("Treat_Pump2", DataTypes.DoubleType).add("Treat_Pump1", DataTypes.DoubleType)
                .add("Treat_LimitSwitch1", DataTypes.DoubleType).add("Dam_Pump3", DataTypes.DoubleType)
                .add("Dam_Pump2", DataTypes.DoubleType).add("Dam_Pump1", DataTypes.DoubleType)
                .add("Dam_LimitSwitch", DataTypes.DoubleType).add("Dam_Chlor_Raw", DataTypes.DoubleType)
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

        PipelineModel model = PipelineModel.load(MODEL_PATH);
        Dataset<Row> predictionDF = model.transform(parsedDF);

        StreamingQuery query = predictionDF.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) realtime_ML::myCustomFunc)
                .option("checkpointLocation", CHECKPOINT_PATH)
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("REALTIME MONITORING ENABLED. LOGGING TO: " + METRICS_CSV);
        query.awaitTermination();
    }
}
