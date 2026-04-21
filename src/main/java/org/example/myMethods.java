package org.example;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class myMethods {
    public static Iterator<Tuple2<String,Long>> wordCountPerDoc(String document) {
        String[] tokens = document.split(" "); // ogni singola parola del doc è un token
        HashMap<String, Long> counts = new HashMap<>();
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        for (String token : tokens) { // conta quante volte è presente una parola e aggiorna l'hashmap
            counts.put(token, 1L + counts.getOrDefault(token, 0L));
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }
    public static Iterator<Tuple2<String,Long>> gatherPairs(Tuple2<Integer,Iterable<Tuple2<String, Long>>> element) {
        HashMap<String, Long> counts = new HashMap<>();
        for (Tuple2<String, Long> c : element._2()) {
            counts.put(c._1(), c._2() + counts.getOrDefault(c._1(), 0L));
        }
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }
}