package HW1;

import org.apache.spark.api.java.JavaRDD;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FairMap {
    public static ArrayList<Point> MRFairFFT (JavaRDD<Point> data, int kA, int kB) {
        // --- ROUND 1 ---
        JavaRDD<Point> localCentersRDD = data.mapPartitions(partition -> {
            ArrayList<Point> points = new ArrayList<>();
            while (partition.hasNext()) {
                points.add(partition.next());
            }
            // Esegue Fair-FFT locale e restituisce l'iteratore dell'ArrayList
            return FairCenter.FairFFT(points, kA, kB).iterator();
        });
        // --- ROUND 2 ---
        // collect() restituisce List, la convertiamo in ArrayList per il driver
        List<Point> collectedList = localCentersRDD.collect();
        ArrayList<Point> coreset = new ArrayList<>(collectedList);

        // Risultato finale
        return FairCenter.FairFFT(coreset, kA, kB);
    }
}