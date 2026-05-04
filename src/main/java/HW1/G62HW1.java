package HW1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import java.util.ArrayList;

public class G62HW1 {
    public static void main(String[] args) {

        // ---- CHECK INPUT ----
        if (args.length != 4) {
            System.err.println("Usage: <filePath> <kA> <kB> <L>");
            System.exit(1);
        }

        // ---- RETRIEVING THE INPUT INFO ----
        String inputPath = args[0];
        int kA = Integer.parseInt(args[1]);
        int kB = Integer.parseInt(args[2]);
        int L  = Integer.parseInt(args[3]);

        // ---- SPARK SETUP ----
        SparkConf conf = new SparkConf().setAppName("G62HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ---- READ INPUT ----
        JavaRDD<String> lines = sc.textFile(inputPath).repartition(L);

        JavaRDD<Tuple2<Vector, Character>> inputPoints = lines.map(line -> {
            String[] tokens = line.split(",");

            // --- RETRIEVING THE COORDINATES OF A POINT ---
            int d = tokens.length - 1;
            double[] coords = new double[d];
            for (int i = 0; i < d; i++) {
                coords[i] = Double.parseDouble(tokens[i]);
            }
            Vector v = Vectors.dense(coords);

            // ---- RETRIEVING THE GROUP OF A POINT ----
            char group = tokens[d].charAt(0);
             return new Tuple2<>(v, group);
        }).cache();

        // ---- COMPUTE N, NA, NB ----
        long N = inputPoints.count();
        long NA = inputPoints.filter(p -> p._2 == 'A').count();
        long NB = inputPoints.filter(p -> p._2 == 'B').count();

        // ---- RUN MRFairFFT (TIMED) ----
        long start = System.currentTimeMillis();
        ArrayList<Tuple2<Vector, Character>> S = FairMap.MRFairFFT(inputPoints, kA, kB);
        long end = System.currentTimeMillis();

        // ---- CALCULATE OBJ FUNC ----
        final ArrayList<Tuple2<Vector, Character>> finalCenters = S;
        double objective = inputPoints.map(p -> {
            double minDist = Double.MAX_VALUE;
            for (Tuple2<Vector, Character> c : finalCenters) {
                double d = Math.sqrt(Vectors.sqdist(p._1, c._1));
                if (d < minDist) minDist = d;
            }
            return minDist;
        }).reduce((d1, d2) -> Math.max(d1, d2));

        // ---- PRINT CMD ARGUMENTS ----
        System.out.println("Input file: " + inputPath + ", KA: " + kA + ", KB: " + kB + ", L: " + L);

        // ---- PRINT NUMB OF POINTS ----
        System.out.println("N  = " + N + ", NA = " + NA + ", NB = " + NB);

        // ---- PRINT CENTER ----
        for (Tuple2<Vector, Character> p : S) {
            System.out.println("Center = " + p._1 + " Label = " + p._2);
        }

        // ---- PRINT OBJ FUNC ----
        System.out.println("Objective function value = " + objective);

        // ---- PRINT TIME ----
        System.out.println("Running time of MRFairFFT = " + (end - start) + " ms");

        sc.close();
    }
}
