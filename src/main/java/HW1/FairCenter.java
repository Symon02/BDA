package HW1;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import java.util.ArrayList;

public class FairCenter {
    public static ArrayList<Tuple2<Vector, Character>> FairFFT(ArrayList<Tuple2<Vector, Character>> points, int kA, int kB) {
        ArrayList<Tuple2<Vector, Character>> centers = new ArrayList<>();
        int countA = 0, countB = 0;

        if (points.isEmpty()) return centers;

        // Inizializziamo le distanze minime di ogni punto dai centri selezionati
        double[] minDistances = new double[points.size()];
        for (int i = 0; i < points.size(); i++) minDistances[i] = Double.MAX_VALUE;

        while (countA < kA || countB < kB) {
            int bestIdx = -1;
            double maxDist = -1.0;

            for (int i = 0; i < points.size(); i++) {
                Tuple2<Vector, Character> p = points.get(i);

                // Controlla se il gruppo ha ancora budget
                boolean canPick = (p._2 == 'A' && countA < kA) ||
                        (p._2 == 'B' && countB < kB);

                if (canPick && minDistances[i] > maxDist) {
                    maxDist = minDistances[i];
                    bestIdx = i;
                }
            }

            if (bestIdx == -1) break;

            Tuple2<Vector, Character> newCenter = points.get(bestIdx);
            centers.add(newCenter);
            if (newCenter._2 == 'A') countA++; else countB++;

            // ---- UPDATE MIN DIST FOR EACH POINT
            for (int i = 0; i < points.size(); i++) {
                double d = Math.sqrt(Vectors.sqdist(points.get(i)._1, newCenter._1));
                if (d < minDistances[i]) {
                    minDistances[i] = d;
                }
            }
        }
        return centers;
    }
}
