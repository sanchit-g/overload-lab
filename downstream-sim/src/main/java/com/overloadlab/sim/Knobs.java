package com.overloadlab.sim;

/**
 * Runtime-mutable fault knobs. Replaced wholesale via POST /control so a scenario
 * can shift the fault mid-run at a known timestamp.
 *
 * mode NORMAL   - sleep latencyMs +/- jitterMs, then respond
 * mode BLACKHOLE - accept the connection and never respond
 */
public record Knobs(long latencyMs, long jitterMs, double failureRate, String mode) {

    public static Knobs healthy() {
        return new Knobs(25, 10, 0.0, "NORMAL");
    }

    public boolean blackhole() {
        return "BLACKHOLE".equalsIgnoreCase(mode);
    }
}
