package HW1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class FairMap {
    public static ArrayList<Tuple2<Vector, Character>> MRFairFFT (JavaRDD<Tuple2<Vector, Character>> data, int kA, int kB) {
        // --- ROUND 1 ---
        JavaRDD<Tuple2<Vector, Character>> localCentersRDD = data.mapPartitions(partition -> {
            ArrayList<Tuple2<Vector, Character>> points = new ArrayList<>();
            while (partition.hasNext()) {
                points.add(partition.next());
            }
            // Esegue Fair-FFT locale e restituisce l'iteratore dell'ArrayList
            return FairCenter.FairFFT(points, kA, kB).iterator();
        });
        // --- ROUND 2 ---
        // collect() restituisce List, la convertiamo in ArrayList per il driver
        List<Tuple2<Vector, Character>> collectedList = localCentersRDD.collect();
        ArrayList<Tuple2<Vector, Character>> coreset = new ArrayList<>(collectedList);

        // Risultato finale
        return FairCenter.FairFFT(coreset, kA, kB);
    }
}