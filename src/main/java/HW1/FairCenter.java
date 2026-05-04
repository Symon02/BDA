package HW1;

import org.apache.spark.mllib.linalg.Vectors;
import java.util.ArrayList;

public class FairCenter {
    public static ArrayList<Point> FairFFT(ArrayList<Point> points, int kA, int kB) {
        ArrayList<Point> centers = new ArrayList<>();
        int countA = 0, countB = 0;

        if (points.isEmpty()) return centers;

        // Inizializziamo le distanze minime di ogni punto dai centri selezionati
        double[] minDistances = new double[points.size()];
        for (int i = 0; i < points.size(); i++) minDistances[i] = Double.MAX_VALUE;

        while (countA < kA || countB < kB) {
            int bestIdx = -1;
            double maxDist = -1.0;

            for (int i = 0; i < points.size(); i++) {
                Point p = points.get(i);

                // Controlla se il gruppo ha ancora budget
                boolean canPick = (p.group == 'A' && countA < kA) ||
                        (p.group == 'B' && countB < kB);

                if (canPick && minDistances[i] > maxDist) {
                    maxDist = minDistances[i];
                    bestIdx = i;
                }
            }

            if (bestIdx == -1) break; // Non ci sono più punti validi

            Point newCenter = points.get(bestIdx);
            centers.add(newCenter);
            if (newCenter.group == 'A') countA++; else countB++;

            // Aggiorna le distanze minime incrementalmente
            for (int i = 0; i < points.size(); i++) {
                double d = Math.sqrt(Vectors.sqdist(points.get(i).p, newCenter.p));
                if (d < minDistances[i]) {
                    minDistances[i] = d;
                }
            }
        }
        return centers;
    }
}
