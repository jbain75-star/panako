/* Copyright (C) 2026 AudioScout. Licensed under AGPL-3.0-or-later.
 * See license.txt. No warranty. */
package be.panako.strategy.panako;

import java.util.*;

/** Bounded, deterministic consensus fit for a candidate's time and pitch. */
public final class RobustAlignment {
    private RobustAlignment() {}

    public static final class Fit {
        public final float slope, offset, frequencyDelta;
        public final int count;
        final double residual;
        Fit(double slope, double offset, double frequencyDelta, int count, double residual) {
            this.slope = (float) slope;
            this.offset = (float) offset;
            this.frequencyDelta = (float) frequencyDelta;
            this.count = count;
            this.residual = residual;
        }
    }

    /** The best supported fit, or null when none qualifies. */
    public static Fit fit(int[] query, int[] reference, int[] frequencyDelta,
            int minimum, double tolerance, double minTime, double maxTime,
            double minFrequency, double maxFrequency, double bandsPerOctave) {
        List<Fit> fits = fits(query, reference, frequencyDelta, minimum, tolerance,
                minTime, maxTime, minFrequency, maxFrequency, bandsPerOctave);
        return fits.isEmpty() ? null : fits.get(0);
    }

    /** Every distinct supported fit, most aligned hits first, then lowest residual. */
    public static List<Fit> fits(int[] query, int[] reference, int[] frequencyDelta,
            int minimum, double tolerance, double minTime, double maxTime,
            double minFrequency, double maxFrequency, double bandsPerOctave) {
        int n = query.length;
        if (reference.length != n || frequencyDelta.length != n)
            throw new IllegalArgumentException("Alignment arrays differ in length");
        if (minimum < 6 || n < minimum || tolerance < 0 || !Double.isFinite(tolerance)) return Collections.emptyList();
        int low = Arrays.stream(query).min().orElse(0), high = Arrays.stream(query).max().orElse(0);
        if (high <= low) return Collections.emptyList();
        Search search = new Search(query, reference, frequencyDelta, minimum, tolerance,
                minTime, maxTime, minFrequency, maxFrequency, bandsPerOctave);

        // Smooth neighbouring offsets: small timing jitter must not split a
        // genuine consensus while a single repeated beat wins an exact-bin tie.
        Map<Integer, Integer> histogram = new HashMap<>();
        for (int i = 0; i < n; i++) histogram.merge(reference[i] - query[i], 1, Integer::sum);
        int radius = Math.min(8, (int) Math.ceil(tolerance));
        List<int[]> peaks = new ArrayList<>();
        for (int offset : histogram.keySet()) {
            int votes = 0;
            for (int d = -radius; d <= radius; d++) votes += histogram.getOrDefault(offset + d, 0);
            peaks.add(new int[] {offset, votes});
        }
        peaks.sort((a, b) -> a[1] == b[1] ? Integer.compare(a[0], b[0]) : Integer.compare(b[1], a[1]));
        for (int i = 0; i < Math.min(8, peaks.size()); i++) {
            int offset = peaks.get(i)[0];
            int[] frequencies = new int[n]; int used = 0;
            for (int j = 0; j < n; j++)
                if (Math.abs(reference[j] - query[j] - offset) <= tolerance * 2)
                    frequencies[used++] = frequencyDelta[j];
            if (used < minimum) continue;
            Arrays.sort(frequencies, 0, used);
            search.consider(0, offset, frequencies[used / 2]);
        }

        // A fixed seed makes identical input reproducible. Widely separated
        // pairs propose a tempo estimate; the entire hit list decides whether
        // it is supported. No first/last-half mode is trusted on its own.
        SplittableRandom random = new SplittableRandom(0x6a09e667f3bcc909L);
        double separation = Math.max(4, (high - low) / 4.0);
        for (int attempt = 0; attempt < 512; attempt++) {
            int a = random.nextInt(n);
            int b = random.nextInt(n);
            double dx = query[b] - query[a];
            if (Math.abs(dx) < separation || Math.abs(frequencyDelta[a] - frequencyDelta[b]) > 2) continue;
            double slope = ((reference[b] - query[b]) - (reference[a] - query[a])) / dx;
            search.consider(slope, reference[a] - query[a] - slope * query[a],
                    (frequencyDelta[a] + frequencyDelta[b]) / 2.0);
        }
        // Stable: equal count and residual keep discovery order.
        search.fits.sort((a, b) -> a.count != b.count ? Integer.compare(b.count, a.count)
                : Double.compare(a.residual, b.residual));
        return search.fits;
    }

    private static final class Search {
        final int[] q, r, f;
        final int minimum;
        final double tolerance, minTime, maxTime, minFrequency, maxFrequency, bands;
        final List<Fit> fits = new ArrayList<>();
        final Set<List<Integer>> seen = new HashSet<>();
        Search(int[] q, int[] r, int[] f, int minimum, double tolerance,
                double minTime, double maxTime, double minFrequency, double maxFrequency, double bands) {
            this.q=q; this.r=r; this.f=f; this.minimum=minimum; this.tolerance=tolerance;
            this.minTime=minTime; this.maxTime=maxTime; this.minFrequency=minFrequency;
            this.maxFrequency=maxFrequency; this.bands=bands;
        }
        boolean allowed(double slope, double frequency) {
            double time = 1 / (1 - slope), pitch = Math.pow(2, frequency / bands);
            return Double.isFinite(time) && time > minTime && time < maxTime
                    && pitch > minFrequency && pitch < maxFrequency;
        }
        void consider(double slope, double offset, double frequency) {
            if (!allowed(slope, frequency)) return;
            // Refine a coarse hypothesis, then repeatedly trim to the same
            // timing tolerance as the original matcher. No acceptance floor
            // or allowable tempo/pitch range is relaxed.
            for (int pass = 0; pass < 3; pass++) {
                double sx=0, sy=0, sxx=0, sxy=0; int count=0;
                double range = pass == 0 ? tolerance * 2 : tolerance;
                for (int i = 0; i < q.length; i++) {
                    double y = r[i] - q[i];
                    if (Math.abs(y - slope*q[i] - offset) > range || Math.abs(f[i]-frequency) > 2) continue;
                    double x=q[i]; sx+=x; sy+=y; sxx+=x*x; sxy+=x*y; count++;
                }
                if (count < minimum) return;
                double denominator = count*sxx-sx*sx;
                if (denominator <= 0) return;
                slope=(count*sxy-sx*sy)/denominator;
                offset=(sy-slope*sx)/count;
                if (!allowed(slope, frequency)) return;
            }
            // Evaluate using the same float coefficients the query consumes.
            float fs=(float)slope, fo=(float)offset;
            int count=0; double residual=0;
            Set<Integer> times = new HashSet<>();
            for (int i=0; i<q.length; i++) {
                double error=Math.abs((r[i]-q[i])-(fs*q[i]+fo));
                if (error <= tolerance && Math.abs(f[i]-frequency) <= 2) {
                    count++; residual+=error; times.add(q[i]);
                }
            }
            // Repeated copies of one print are not independent timing evidence.
            if (count < minimum || times.size() < Math.min(12, minimum)) return;
            Fit fit = new Fit(fs,fo,frequency,count,residual);
            if (seen.add(Arrays.asList(Float.floatToIntBits(fit.slope), Float.floatToIntBits(fit.offset),
                    Float.floatToIntBits(fit.frequencyDelta))))
                fits.add(fit);
        }
    }
}
