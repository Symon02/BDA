package HW1;

import org.apache.spark.mllib.linalg.Vector;

public class Point {
        Vector p;
        char group; // 'A' or 'B'

        Point(Vector p, char group) {
            this.p = p;
            this.group = group;
        }
    }

