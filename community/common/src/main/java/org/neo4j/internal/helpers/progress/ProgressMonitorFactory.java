/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.helpers.progress;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.neo4j.time.Clocks;
import org.neo4j.util.Preconditions;

public abstract class ProgressMonitorFactory {
    public static final ProgressMonitorFactory NONE = new ProgressMonitorFactory() {
        @Override
        protected Indicator newIndicator(String process) {
            return Indicator.NONE;
        }
    };

    /**
     * Creates a textual indicator that outputs to the OutputStream passed in.
     * A textual indicator will output progress using grouped dots and percentages on multiple lines.
     * The output will look like:<br>
     * ................... 10%<br>
     * ................... 20%<br>
     * <...><br>
     * ................... 100%<br>
     * <p>
     * For more control over how to display progress, look at {@link #textual(OutputStream, boolean, int, int, int)}
     * </p>
     * <p>
     * For a simple indicator that just outputs progress numbers, use {@link #textual textual}
     * </p>
     * @param out {@link OutputStream OutputStream} to write to.
     * @return {@link ProgressMonitorFactory}
     */
    public static ProgressMonitorFactory textual(final OutputStream out) {
        return textual(
                new OutputStreamWriter(out, StandardCharsets.UTF_8),
                false,
                TextualIndicator.DEFAULT_DOTS_PER_GROUP,
                TextualIndicator.DEFAULT_GROUPS_PER_LINE,
                TextualIndicator.DEFAULT_NUM_LINES);
    }

    /**
     * Creates a basicTextual indicator that outputs to the OutputStream passed in.
     * A textual indicator will output progress as a simple percentage.
     * <p>
     * For a more advanced indicator, use {@link #textual(OutputStream, boolean, int, int, int)}
     * </p>
     * @param out {@link OutputStream OutputStream} to write to.
     * @return {@link ProgressMonitorFactory}
     */
    public static ProgressMonitorFactory basicTextual(final Writer out) {
        return basicTextual(
                out,
                BasicTextualIndicator.DEFAULT_RESOLUTION,
                BasicTextualIndicator.DEFAULT_DISPLAY_STEP,
                BasicTextualIndicator.DEFAULT_DISPLAY_TEXT);
    }

    /**
     * Creates a textual indicator that outputs to the Writer passed in.
     * A textual indicator will output progress using grouped dots and percentages on multiple lines.
     * The output will look like:<br>
     * ................... 10%<br>
     * ................... 20%<br>
     * <...><br>
     * ................... 100%<br>
     * <p>
     * For more control over how to display progress, look at {@link #textual(OutputStream, boolean, int, int, int)}
     * </p>
     * <p>
     * For a simple indicator that just outputs progress numbers, use {@link #textual textual}
     * </p>
     * @param out {@link Writer Writer} to write to.
     * @return {@link ProgressMonitorFactory}
     */
    public static ProgressMonitorFactory textual(final Writer out) {
        return textual(
                out,
                false,
                TextualIndicator.DEFAULT_DOTS_PER_GROUP,
                TextualIndicator.DEFAULT_GROUPS_PER_LINE,
                TextualIndicator.DEFAULT_NUM_LINES);
    }

    /**
     * Creates a textual indicator that outputs to the OutputStream passed in.
     * A textual indicator will output progress using grouped dots and percentages on multiple lines.
     * Here is an example of output:<br>
     * ..... ..... ..... 20%<br>
     * ..... ..... ..... 40%<br>
     * ..... ..... ..... 60%<br>
     * ..... ..... ..... 80%<br>
     * ..... ..... ..... 100%<br>
     * <p>
     * For a simple indicator that just outputs progress numbers, use {@link #textual textual}
     * </p>
     * @param out {@link OutputStream OutputStream} to write to.
     * @param deltaTimes Enable/disable timestamps.
     * @param dotsPerGroup How many dots we have in each group. In the above example, it is 5.
     * @param groupsPerLine How many groups of dots per line. In the above example, it i 3
     * @param numLines How many lines, each ending with a percentage. In the above example it is 5.
     * @return {@link ProgressMonitorFactory}
     */
    public static ProgressMonitorFactory textual(
            final OutputStream out, boolean deltaTimes, int dotsPerGroup, int groupsPerLine, int numLines) {
        return textual(
                new OutputStreamWriter(out, StandardCharsets.UTF_8), deltaTimes, dotsPerGroup, groupsPerLine, numLines);
    }

    /**
     * Creates a textual indicator that outputs to the Writer passed in.
     * A textual indicator will output progress using grouped dots and percentages on multiple lines.
     * Here is an example of output:<br>
     * ..... ..... ..... 20%<br>
     * ..... ..... ..... 40%<br>
     * ..... ..... ..... 60%<br>
     * ..... ..... ..... 80%<br>
     * ..... ..... ..... 100%<br>
     * <p>
     * For a simple indicator that just outputs progress numbers, use {@link #textual textual}
     * </p>
     * @param out {@link Writer Writer} to write to.
     * @param deltaTimes Enable/disable timestamps.
     * @param dotsPerGroup How many dots we have in each group. In the above example, it is 5.
     * @param groupsPerLine How many groups of dots per line. In the above example, it i 3
     * @param numLines How many lines, each ending with a percentage. In the above example it is 5.
     * @return {@link ProgressMonitorFactory}
     */
    public static ProgressMonitorFactory textual(
            final Writer out, boolean deltaTimes, int dotsPerGroup, int groupsPerLine, int numLines) {
        return new ProgressMonitorFactory() {
            @Override
            protected Indicator newIndicator(String process) {
                return new TextualIndicator(
                        process,
                        writer(),
                        deltaTimes,
                        Clocks.nanoClock(),
                        TextualIndicator.DEFAULT_DELTA_CHARACTER,
                        dotsPerGroup,
                        groupsPerLine,
                        numLines);
            }

            private PrintWriter writer() {
                return out instanceof PrintWriter ? (PrintWriter) out : new PrintWriter(out);
            }
        };
    }

    public static ProgressMonitorFactory basicTextual(final Writer out, int resolution, int step, String displayText) {
        return new ProgressMonitorFactory() {
            @Override
            protected Indicator newIndicator(String process) {
                return new BasicTextualIndicator(process, writer(), resolution, step, displayText);
            }

            private PrintWriter writer() {
                return out instanceof PrintWriter ? (PrintWriter) out : new PrintWriter(out);
            }
        };
    }

    /**
     * A way to, instead of e.g. printing dots, call {@link ProgressListener#add(long)}
     * which can be used to map a progress report from one range to another.
     *
     * @param target {@link ProgressListener} to use as target 'dot' receiver.
     * @param resolution number of 'dots' in the target progress which the source progress writes.
     * @return ProgressMonitorFactory able to map a progress report from one range to another.
     */
    public static ProgressMonitorFactory mapped(ProgressListener target, int resolution) {
        return new ProgressMonitorFactory() {
            @Override
            protected Indicator newIndicator(String process) {
                return new Indicator(resolution) {
                    @Override
                    protected void progress(int from, int to) {
                        // Even tho the Indicator synchronizes internally around this call there may be
                        // multiple mapped progresses on the same target so synchronize on the target.
                        // The resolution is typically/hopefully quite small (like 100 or 1000 or so),
                        // so the synchronization cost should still be minimal.
                        synchronized (target) {
                            target.add(to - from);
                        }
                    }
                };
            }
        };
    }

    public final MultiPartBuilder multipleParts(String process) {
        return new MultiPartBuilder(newIndicator(process));
    }

    public final ProgressListener singlePart(String process, long totalCount) {
        return new ProgressListener.SinglePartProgressListener(newIndicator(process), totalCount);
    }

    protected abstract Indicator newIndicator(String process);

    public static class MultiPartBuilder {
        private Aggregator aggregator;
        private Set<String> parts = new HashSet<>();

        private MultiPartBuilder(Indicator indicator) {
            this.aggregator = new Aggregator(indicator);
        }

        public ProgressListener progressForPart(String part, long totalCount) {
            assertNotBuilt();
            assertUniquePart(part);
            ProgressListener.MultiPartProgressListener progress =
                    new ProgressListener.MultiPartProgressListener(aggregator, part, totalCount);
            aggregator.add(progress, totalCount);
            return progress;
        }

        private void assertUniquePart(String part) {
            if (!parts.add(part)) {
                throw new IllegalArgumentException(String.format("Part '%s' has already been defined.", part));
            }
        }

        private void assertNotBuilt() {
            if (aggregator == null) {
                throw new IllegalStateException("Builder has been completed.");
            }
        }

        /**
         * Have to be called after all individual progresses have been added.
         * @return a {@link Completer} which can be called do issue {@link ProgressListener#close()} for all individual progress parts.
         */
        public Completer build() {
            Preconditions.checkState(aggregator != null, "Already built");
            Completer completer = aggregator.initialize();
            aggregator = null;
            parts = null;
            return completer;
        }

        /**
         * Can be called to invoke all individual {@link ProgressListener#close()}.
         */
        public void done() {
            aggregator.done();
        }
    }

    public interface Completer extends AutoCloseable {
        @Override
        void close();
    }
}
