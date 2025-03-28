import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.RowFactory;
import scala.Tuple3;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName());
        conf = conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("2ID70")
                .getOrCreate();

        Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds = Q1(sc, spark);

        Q2(spark);
        Q3(rdds);
        Q4(rdds);
    }

    public static Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> Q1(JavaSparkContext sc, SparkSession spark) {
        // Load data into RDDs
        JavaRDD<String> patientsRDD = sc.textFile("src/main/resources/assignment2_data_v2/patients.csv"); // fields: patientId int, patientName character(100), address character(200), dateOfBirth character(10)
        JavaRDD<String> prescriptionsRDD = sc.textFile("src/main/resources/assignment2_data_v2/prescriptions.csv"); // fields: prescriptionId int, medicineId int, dosage character(100)
        JavaRDD<String> diagnosesRDD = sc.textFile("src/main/resources/assignment2_data_v2/diagnoses.csv"); // fields: patientId int, doctorId int, date character(10), diagnosis character(200), prescriptionId int
        int[] expectedLengths = {4,3,5};

        // Filter invalid lines in patientsRDD
        JavaRDD<String> filteredPatientsRDD = patientsRDD.filter(line -> {
            String[] parts = line.split(",");
            if (parts.length != expectedLengths[0]) return false;
            try {
                Integer.parseInt(parts[0]);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        });

        // Define schema for patients DataFrame
        StructType patientsSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("patientId", DataTypes.IntegerType, false),
                DataTypes.createStructField("patientName", DataTypes.StringType, false),
                DataTypes.createStructField("address", DataTypes.StringType, false),
                DataTypes.createStructField("dateOfBirth", DataTypes.StringType, false)
        });

        StructType prescriptionsSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("prescriptionId", DataTypes.IntegerType, false),
                DataTypes.createStructField("medicineId", DataTypes.IntegerType, false),
                DataTypes.createStructField("dosage", DataTypes.StringType, false)
        });

        StructType diagnosesSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("patientId", DataTypes.IntegerType, false),
                DataTypes.createStructField("doctorId", DataTypes.IntegerType, false),
                DataTypes.createStructField("date", DataTypes.StringType, false),
                DataTypes.createStructField("diagnosis", DataTypes.StringType, false),
                DataTypes.createStructField("prescriptionId", DataTypes.IntegerType, false)
        });

        // Convert RDD to DataFrame
        Dataset<Row> patientsDF = spark.createDataFrame(filteredPatientsRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), parts[1], parts[2], parts[3]);
        }), patientsSchema);

        Dataset<Row> prescriptionsDF = spark.createDataFrame(prescriptionsRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), parts[2]);
        }), prescriptionsSchema);

        Dataset<Row> diagnosesDF = spark.createDataFrame(diagnosesRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), parts[2], parts[3], Integer.parseInt(parts[4]));
        }), diagnosesSchema);

        // Register DataFrame as a temporary view
        patientsDF.createOrReplaceTempView("patients");
        prescriptionsDF.createOrReplaceTempView("prescriptions");
        diagnosesDF.createOrReplaceTempView("diagnoses");

        //print line count
//        System.out.println(">> [patientsRDD: " + filteredPatientsRDD.count() + "]");
//        System.out.println(">> [prescriptionsRDD: " + prescriptionsRDD.count() + "]");
//        System.out.println(">> [diagnosesRDD: " + diagnosesRDD.count() + "]");

        return new Tuple3<>(filteredPatientsRDD, prescriptionsRDD, diagnosesRDD);
    }


    public static void Q2(SparkSession spark) {
        // Query 1: Find the number of patients that were born in 1999
        Dataset<Row> q21Result = spark.sql(
                "SELECT COUNT(*) AS count FROM patients WHERE dateOfBirth LIKE '1999-%'"
        );
        long q21 = q21Result.first().getLong(0);
        System.out.println(">> [q21: " + q21 + "]");

        // Query 2: Find the date in 2024 where the number of diagnoses reached its maximum value
        Dataset<Row> q22Result = spark.sql(
                "SELECT date, COUNT(*) AS count FROM diagnoses WHERE date LIKE '2024-%' GROUP BY date ORDER BY count DESC LIMIT 1"
        );
        String q22 = q22Result.first().getString(0);
        System.out.println(">> [q22: " + q22 + "]");

        // Query 3: Find the date in 2024 where the prescription with the maximum number of medicines was administered
        Dataset<Row> q23Result = spark.sql(
                "SELECT d.date, COUNT(p.prescriptionId) AS count " +
                        "FROM diagnoses d " +
                        "JOIN prescriptions p ON d.prescriptionId = p.prescriptionId " +
                        "WHERE d.date LIKE '2024-%' " +
                        "GROUP BY d.date " +
                        "ORDER BY count DESC " +
                        "LIMIT 1"
        );
        String q23 = q23Result.first().getString(0);
        System.out.println(">> [q23: " + q23 + "]");

    }

    public static void Q3(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        var q31 = 0;
        System.out.println(">> [q31: " + q31 + "]");

        var q32 = 0;
        System.out.println(">> [q32: " + q32 + "]");

        var q33 = 0;
        System.out.println(">> [q33: " + q33 + "]");
    }

    public static void Q4(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        var q4 = 0;
        System.out.println(">> [q4: " + q4 + "]");
    }
}
