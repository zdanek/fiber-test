package com.github.vy.fibertest;

import org.junit.Test;

public class VertxRingBenchmarkTest extends VertxRingBenchmark {
/*
    @BeforeClass
    public static void setup() {
        System.setProperty("workerCount", "5");
        System.setProperty("ringSize", "500");
    }
*/
    @Test
    public void testRingBenchmark() throws Exception {
        Util.testRingBenchmark(workerCount, ringSize, ringBenchmark());
    }

}
