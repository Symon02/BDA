package HW1;

import org.apache.spark.mllib.linalg.Vectors;
import java.util.ArrayList;
import java.util.Random;


public class FairKCenter {
    public static ArrayList<Point> FairFFT(ArrayList<Point> U, int kA, int kB) {
        ArrayList<Point> S = new ArrayList<>();
        Random rand = new Random();

        // pick a random starting point
        Point first = U.get(rand.nextInt(U.size()));
        S.add(first);

        int countA = (first.group == 'A') ? 1 : 0;
        int countB = (first.group == 'B') ? 1 : 0;

        while (S.size() < kA + kB) {
            Point farthestPoint = null;
            double maxDist = -1;

            for (Point candidate : U) {
                // check that we don't select too many point of one group
                if ((candidate.group == 'A' && countA >= kA) || (candidate.group == 'B' && countB >= kB)) {
                    continue;
                }

                double minDist = Double.MAX_VALUE;

                for (Point center : S) {
                    double dist = Vectors.sqdist(candidate.p, center.p);
                    minDist = Math.min(minDist, dist);
                }

                if (minDist > maxDist) {
                    maxDist = minDist;
                    farthestPoint = candidate;
                }
            }

            if (farthestPoint == null) break;

            S.add(farthestPoint);

            // updating the number of points of each group added to the centers
            if (farthestPoint.group == 'A') {
                countA++;
            } else {
                countB++;
            }

        }

        return S;
    }
}
