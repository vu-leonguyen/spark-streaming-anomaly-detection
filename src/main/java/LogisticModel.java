import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;

import org.apache.spark.ml.classification.LogisticRegression;

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

import org.apache.spark.ml.feature.VectorAssembler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.FileWriter;
import java.io.PrintWriter;

public class LogisticModel {

    // =====================================================
    // PATHS
    // =====================================================

    private static final String MODEL_PATH =
            "/home/doringu123/SparkKafkaConsumer/models/waterlog/logistic";

    private static final String DATASET_PATH =
            "/home/doringu123/SparkKafkaConsumer/datasets/Dataset.csv";

    private static final String METRICS_PATH =
            "/home/doringu123/SparkKafkaConsumer/results/model_metrics.txt";

    // =====================================================
    // MAIN
    // =====================================================

    public static void main(String[] args) throws Exception {

        // ==============================================
        // REDUCE LOGS
        // ==============================================

        Logger.getLogger("org").setLevel(Level.ERROR);

        Logger.getLogger("akka").setLevel(Level.ERROR);

        Logger.getRootLogger().setLevel(Level.ERROR);

        // ==============================================
        // SPARK SESSION
        // ==============================================

        SparkSession spark = SparkSession.builder()

                .appName("WaterLog-Training")

                .master("local[*]")

                .config("spark.driver.memory", "4g")

                .config("spark.executor.memory", "4g")

                .config("spark.ui.showConsoleProgress", "false")

                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // ==============================================
        // DEFINE SCHEMA
        // ==============================================

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

        // ==============================================
        // LOAD DATASET
        // ==============================================

        Dataset<Row> df = spark.read()

                .option("header", "true")

                .schema(schema)

                .csv(DATASET_PATH);

        System.out.println("======================================");
        System.out.println("DATASET LOADED");
        System.out.println("TOTAL ROWS: " + df.count());
        System.out.println("======================================");

        // ==============================================
        // FEATURE ASSEMBLER
        // ==============================================

        VectorAssembler assembler = new VectorAssembler()

                .setInputCols(new String[]{

                        "Treat_ScenarioID",
                        "Treat_Level2",
                        "Treat_Level1",
                        "Treat_CurrentVolume",

                        "Dam_Level",
                        "Dam_CurrentVolume",

                        "Treat_PumpFlowLiterMin",

                        "Treat_Pump3",
                        "Treat_Pump2",
                        "Treat_Pump1",

                        "Treat_LimitSwitch1",

                        "Dam_Pump3",
                        "Dam_Pump2",
                        "Dam_Pump1",

                        "Dam_LimitSwitch",

                        "Dam_Chlor_Raw"
                })

                .setOutputCol("features")

                .setHandleInvalid("keep");

        // ==============================================
        // LOGISTIC REGRESSION
        // ==============================================

        LogisticRegression lr = new LogisticRegression()

                .setFeaturesCol("features")

                .setLabelCol("label")

                .setMaxIter(100)

                .setRegParam(0.01)

                .setElasticNetParam(0.1);

        // ==============================================
        // PIPELINE
        // ==============================================

        Pipeline pipeline = new Pipeline()

                .setStages(new PipelineStage[]{
                        assembler,
                        lr
                });

        // ==============================================
        // TRAIN / TEST SPLIT
        // ==============================================

        Dataset<Row>[] splitData =
                df.randomSplit(new double[]{0.8, 0.2}, 42);

        Dataset<Row> trainingData = splitData[0];

        Dataset<Row> testData = splitData[1];

        System.out.println("======================================");
        System.out.println("TRAINING ROWS: " + trainingData.count());
        System.out.println("TEST ROWS: " + testData.count());
        System.out.println("======================================");

        // ==============================================
        // TRAIN MODEL
        // ==============================================

        System.out.println("======================================");
        System.out.println("TRAINING MODEL...");
        System.out.println("======================================");

        PipelineModel model =
                pipeline.fit(trainingData);

        // ==============================================
        // PREDICTIONS
        // ==============================================

        Dataset<Row> predictions =
                model.transform(testData);

        // ==============================================
        // EVALUATION
        // ==============================================

        MulticlassClassificationEvaluator evaluator =
                new MulticlassClassificationEvaluator()

                        .setLabelCol("label")

                        .setPredictionCol("prediction");

        double accuracy = evaluator
                .setMetricName("accuracy")
                .evaluate(predictions);

        double precision = evaluator
                .setMetricName("weightedPrecision")
                .evaluate(predictions);

        double recall = evaluator
                .setMetricName("weightedRecall")
                .evaluate(predictions);

        double f1 = evaluator
                .setMetricName("f1")
                .evaluate(predictions);

        // ==============================================
        // CONFUSION MATRIX
        // ==============================================

        Dataset<Row> confusionMatrix =
                predictions.groupBy("label", "prediction")
                        .count()
                        .orderBy("label", "prediction");

        // ==============================================
        // PRINT RESULTS
        // ==============================================

        System.out.println("======================================");
        System.out.println("MODEL EVALUATION");
        System.out.println("======================================");

        System.out.println("Accuracy  : " + accuracy);

        System.out.println("Precision : " + precision);

        System.out.println("Recall    : " + recall);

        System.out.println("F1 Score  : " + f1);

        System.out.println("======================================");

        System.out.println("CONFUSION MATRIX");

        confusionMatrix.show();

        // ==============================================
        // SAVE METRICS
        // ==============================================

        try {

            FileWriter fw =
                    new FileWriter(METRICS_PATH);

            PrintWriter pw =
                    new PrintWriter(fw);

            pw.println("======================================");
            pw.println("MODEL METRICS");
            pw.println("======================================");

            pw.println("Accuracy  : " + accuracy);

            pw.println("Precision : " + precision);

            pw.println("Recall    : " + recall);

            pw.println("F1 Score  : " + f1);

            pw.println("======================================");

            pw.flush();

            pw.close();

        } catch (Exception e) {

            e.printStackTrace();
        }

        // ==============================================
        // SAVE MODEL
        // ==============================================

        model.write()

                .overwrite()

                .save(MODEL_PATH);

        System.out.println("======================================");
        System.out.println("MODEL SAVED");
        System.out.println(MODEL_PATH);
        System.out.println("======================================");

        spark.stop();
    }
}
