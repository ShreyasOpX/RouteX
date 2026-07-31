package com.routex.response;

public class BenchmarkResponse {
    private int recordsPublished;
    private long durationMs;
    private double throughputPerSecond;

    public BenchmarkResponse() {

    }

    public BenchmarkResponse(int recordsPublished, long durationMs, double throughputPerSecond) {
        this.recordsPublished = recordsPublished;
        this.durationMs = durationMs;
        this.throughputPerSecond = throughputPerSecond;
    }

    public int getRecordsPublished() {
        return recordsPublished;
    }

    public void setRecordsPublished(int recordsPublished) {
        this.recordsPublished = recordsPublished;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public double getThroughputPerSecond() {
        return throughputPerSecond;
    }

    public void setThroughputPerSecond(double throughputPerSecond) {
        this.throughputPerSecond = throughputPerSecond;
    }
}