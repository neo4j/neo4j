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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.internal.helpers.Format.duration;

import java.io.PrintWriter;
import org.neo4j.time.SystemNanoClock;

class TextualIndicator extends Indicator {
    static final int DEFAULT_DOTS_PER_GROUP = 20;
    static final int DEFAULT_GROUPS_PER_LINE = 1;
    static final int DEFAULT_NUM_LINES = 10;
    static final char DEFAULT_DELTA_CHARACTER = '∆';

    private final String process;
    private final PrintWriter out;
    private final boolean deltaTimes;
    private final SystemNanoClock clock;
    private final char deltaCharacter;
    private final int dotsPerGroup;
    private final int groupsPerLine;
    private final int dotsPerLine;
    private long lastReportTime;
    private long startTime;
    private Character mark;

    TextualIndicator(
            String process,
            PrintWriter out,
            boolean deltaTimes,
            SystemNanoClock clock,
            char deltaCharacter,
            int dotsPerGroup,
            int groupsPerLine,
            int numLines) {
        super(dotsPerGroup * groupsPerLine * numLines);
        this.process = process;
        this.out = out;
        this.deltaTimes = deltaTimes;
        this.clock = clock;
        this.deltaCharacter = deltaCharacter;
        this.dotsPerGroup = dotsPerGroup;
        this.groupsPerLine = groupsPerLine;
        this.dotsPerLine = dotsPerGroup * groupsPerLine;
    }

    @Override
    public void startProcess(long totalCount) {
        out.println(process);
        out.flush();
        lastReportTime = clock.nanos();
        startTime = lastReportTime;
    }

    @Override
    protected void progress(int from, int to) {
        for (int i = from; i < to; ) {
            printProgress(++i);
        }
        out.flush();
    }

    @Override
    public void mark(char mark) {
        this.mark = mark;
    }

    @Override
    public void failure(Throwable cause) {
        cause.printStackTrace(out);
    }

    private void printProgress(int progress) {
        if (groupsPerLine > 1) {
            var lineBasedProgress = (progress - 1) % dotsPerLine;
            if (lineBasedProgress > 0 && (lineBasedProgress % dotsPerGroup) == 0) {
                out.print(' ');
            }
        }
        out.print(progressCharacter());
        if (progress % dotsPerLine == 0) {
            out.printf(" %3d%%", progress * 100 / reportResolution());
            long currentTime = clock.nanos();
            if (deltaTimes) {
                long time = currentTime - lastReportTime;
                long totalTime = currentTime - startTime;
                out.printf(
                        " %c%s [%s]",
                        deltaCharacter,
                        duration(NANOSECONDS.toMillis(time)),
                        duration(NANOSECONDS.toMillis(totalTime)));
            }
            out.printf("%n");
            lastReportTime = currentTime;
        }
    }

    private char progressCharacter() {
        if (mark != null) {
            try {
                return mark;
            } finally {
                mark = null;
            }
        }
        return '.';
    }
}
