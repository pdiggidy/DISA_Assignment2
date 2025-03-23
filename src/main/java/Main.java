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
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

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

//        JavaRDD<String> patientsRDD = sc.textFile("src/main/resources/assignment2_data_small_answers_v2/patients.csv"); // fields: patientId int, patientName character(100), address character(200), dateOfBirth character(10)
//        JavaRDD<String> prescriptionsRDD = sc.textFile("src/main/resources/assignment2_data_small_answers_v2/prescriptions.csv"); // fields: prescriptionId int, medicineId int, dosage character(100)
//        JavaRDD<String> diagnosesRDD = sc.textFile("src/main/resources/assignment2_data_small_answers_v2/diagnoses.csv");
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

        return new Tuple3<>(filteredPatientsRDD, prescriptionsRDD, diagnosesRDD);
    }


    public static void Q2(SparkSession spark) {
        long startTime, endTime;

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
                "WITH prescription_counts AS (" +
                        "SELECT p.prescriptionId, COUNT(*) AS count " +
                        "FROM prescriptions AS p " +
                        "GROUP BY p.prescriptionId ) " +
                        "SELECT d.date " +
                        "FROM diagnoses d " +
                        "JOIN prescription_counts AS pc ON d.prescriptionId = pc.prescriptionId " +
                        "WHERE d.date LIKE '2024-%' " +
                        "ORDER BY count DESC " +
                        "LIMIT 1"
        );
        String q23 = q23Result.first().getString(0);
        System.out.println(">> [q23: " + q23 + "]");
    }


    public static void Q3(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        long startTime, endTime;

        // Query 1: Find the number of patients that were born in 1999
        long q31 = rdds._1().filter(line -> {
            String[] parts = line.split(",");
            return parts[3].startsWith("1999");
        }).count();
        System.out.println(">> [q31: " + q31 + "]");

        // Query 2: Find the date in 2024 where the number of diagnoses reached its maximum value
        JavaPairRDD<String, Integer> diagnosesByDate = rdds._3()
                .filter(line -> {
                    String[] parts = line.split(",");
                    return parts[2].startsWith("2024");
                })
                .mapToPair(line -> {
                    String date = line.split(",")[2];
                    return new Tuple2<>(date, 1);
                })
                .reduceByKey((a, b) -> a + b);
        Tuple2<String, Integer> maxDiagnoses = diagnosesByDate.reduce(
                (t1, t2) -> t1._2 > t2._2 ? t1 : t2);
        String q32 = maxDiagnoses._1();
        System.out.println(">> [q32: " + q32 + "]");

        // Query 3: Find the date in 2024 where the prescription with the maximum number of medicines was administered
        JavaPairRDD<Integer, Integer> prescriptionCounts = rdds._2()
                .mapToPair(line -> {
                    String[] part = line.split(",");
                    int prescriptionId = Integer.parseInt(part[0]);
                    return new Tuple2<>(prescriptionId, 1);
                })
                .reduceByKey((a, b) -> a + b);
        Tuple2<Integer, Integer> maxPrescription = prescriptionCounts.reduce(
                (t1, t2) -> t1._2 > t2._2 ? t1 : t2);
        int maxPrescriptionId = maxPrescription._1();
        String q33 = rdds._3()
                .filter(line -> {
                    String[] parts = line.split(",");
                    return parts[2].startsWith("2024") && Integer.parseInt(parts[4]) == maxPrescriptionId;
                })
                .map(line -> line.split(",")[2])
                .first();
        System.out.println(">> [q33: " + q33 + "]");
    }


    public static void Q4(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {

        // ((month, doctorId), (diagnosis, topDiagnosisCount))
        JavaPairRDD<Tuple2<String, String>, Tuple2<String, Integer>> doctorTopDiagnosis = rdds._3()
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    String month = parts[2].substring(0, 7);
                    String doctorId = parts[1];
                    String diagnosis = parts[3];
                    return new Tuple2<>(new Tuple2<>(month, doctorId), new Tuple2<>(diagnosis, 1));
                })
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1, t1._2 + t2._2))
                .mapToPair(line -> new Tuple2<>(line._1, line._2))
                .reduceByKey((tup1, tup2) -> tup1._2 >= tup2._2 ? tup1 : tup2);

        // (month, topDiagnosis)
        JavaPairRDD<String, String> monthTopDiagnosis = doctorTopDiagnosis
                .mapToPair(line -> new Tuple2<>(line._1._1, line._2._1));

        // ((month, topDiagnosis), doctorsPerDiagnosisCount)
        JavaPairRDD<Tuple2<String, String>, Integer> doctorsPerDiagnosisCount = monthTopDiagnosis
                .mapToPair(line -> new Tuple2<>(line, 1))
                .reduceByKey((a, b) -> a + b);

        // (month, doctorsCount)
        JavaPairRDD<String, Integer> doctorsCount = doctorTopDiagnosis
                .mapToPair(line -> new Tuple2<>(line._1._1, 1))
                .reduceByKey((a, b) -> a + b);

        // (month, (topDiagnosis, doctorsPerDiagnosisCount), doctorsCount)
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> joined = doctorsPerDiagnosisCount
                .mapToPair(line -> new Tuple2<>(line._1._1, new Tuple2<>(line._1._2, line._2)))
                .join(doctorsCount);

        // (month, diagnosis)
        JavaPairRDD<String, String> result = joined
                .filter(line -> line._2._1._2 > line._2._2 / 2)
                .mapToPair(line -> new Tuple2<>(line._1, line._2._1._1));

        String q4 = result
                .sortByKey()
                .map(list -> list._1 + "," + list._2)
                .collect()
                .stream()
                .reduce((a, b) -> a + ";" + b)
                .orElse("");

        System.out.println(">> [q4: " + q4 + "]");
    }
}
