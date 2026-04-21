package HW1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;

public class G62HW1 {
    public static void main(String[] args) {

        // ---- CHECK INPUT ----
        if (args.length != 4) {
            System.err.println("Usage: <filePath> <kA> <kB> <L>");
            System.exit(1);
        }

        String inputPath = args[0];
        int kA = Integer.parseInt(args[1]);
        int kB = Integer.parseInt(args[2]);
        int L  = Integer.parseInt(args[3]);

        // ---- PRINT ARGUMENTS ----
        System.out.println("Input file: " + inputPath);
        System.out.println("kA: " + kA);
        System.out.println("kB: " + kB);
        System.out.println("L: " + L);

        // ---- SPARK SETUP ----
        SparkConf conf = new SparkConf().setAppName("G62HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ---- READ INPUT ----
        JavaRDD<String> lines = sc.textFile(inputPath).repartition(L);

        JavaRDD<Point> inputPoints = lines.map(line -> {
            String[] tokens = line.split(",");

            int d = tokens.length - 1;
            double[] coords = new double[d];

            for (int i = 0; i < d; i++) {
                coords[i] = Double.parseDouble(tokens[i]);
            }

            Vector v = Vectors.dense(coords);
            char group = tokens[d].charAt(0); //retrieve the group of the point -- last thong written in each line

            return new Point(v, group);
        }).cache();

        // ---- COMPUTE N, NA, NB ----
        long N = inputPoints.count();

        long NA = inputPoints.filter(p -> p.group == 'A').count();
        long NB = inputPoints.filter(p -> p.group == 'B').count();

        System.out.println("N  = " + N);
        System.out.println("NA = " + NA);
        System.out.println("NB = " + NB);

        // ---- RUN MRFairFFT (TIMED) ----
        long start = System.currentTimeMillis();

        ArrayList<Point> S = MRFairKCenter.MRFairFFT(inputPoints, kA, kB);

        long end = System.currentTimeMillis();

        // ---- PRINT SOLUTION ----
        System.out.println("\nCenters:");
        for (Point p : S) {
            System.out.println(p.p + "," + p.group);
        }

        // ---- OBJECTIVE FUNCTION ----
        double objective = inputPoints.map(point -> {
            double minDist = Double.MAX_VALUE;

            for (Point center : S) {
                double d = Vectors.sqdist(point.p, center.p);
                minDist = Math.min(minDist, d);
            }

            return minDist;
        }).reduce(Math::max);

        System.out.println("\nObjective function value = " + objective);

        // ---- PRINT TIME ----
        System.out.println("Execution time (ms) = " + (end - start));

        sc.close();
    }
}
