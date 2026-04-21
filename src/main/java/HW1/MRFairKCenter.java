package HW1;


import org.apache.spark.api.java.*;
import scala.Tuple2;

import java.util.ArrayList;

public class MRFairKCenter {
    public static ArrayList<Point> MRFairFFT(JavaRDD<Point> U, int kA, int kB) {

        // -------- ROUND 1 --------
        JavaPairRDD<Integer, Point> coresetPairs = U.mapPartitionsToPair(iter -> {

            ArrayList<Point> partitionPoints = new ArrayList<>();
            iter.forEachRemaining(partitionPoints::add);

            // makes sure that in each partition there are enough point to select the centers. If not, reduce the center needed
            int localKA = Math.min(kA, partitionPoints.size());
            int localKB = Math.min(kB, partitionPoints.size());

            ArrayList<Point> localCenters =
                    FairKCenter.FairFFT(partitionPoints, localKA, localKB);

            // emit (dummyKey, point)
            ArrayList<Tuple2<Integer, Point>> output = new ArrayList<>();
            for (Point p : localCenters) {
                output.add(new Tuple2<>(0, p));
            }

            return output.iterator();
        });

        // discard keys and collect coreset
        JavaRDD<Point> coreset = coresetPairs.values();

        // -------- ROUND 2 --------
        ArrayList<Point> collected = new ArrayList<>(coreset.collect());

        return FairKCenter.FairFFT(collected, kA, kB);
    }
}
