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

import java.io.PrintWriter;

/* This is a simple indicator that only shows a text every X steps e.g. 10% completed */
class BasicTextualIndicator extends Indicator {
    static final int DEFAULT_RESOLUTION = 100;
    static final int DEFAULT_DISPLAY_STEP = 10;
    static final String DEFAULT_DISPLAY_TEXT = " %3d%% completed";

    private final String process;
    private final PrintWriter out;
    private final int step;
    private final String displayText;

    BasicTextualIndicator(String process, PrintWriter out, int resolution, int step, String displayText) {
        super(resolution);
        this.process = process;
        this.out = out;
        this.step = step;
        this.displayText = displayText;
    }

    @Override
    public void startProcess(long totalCount) {
        out.println(process);
        out.flush();
    }

    @Override
    protected void progress(int from, int to) {
        for (int i = from; i < to; ) {
            printProgress(++i);
        }
        out.flush();
    }

    @Override
    public void failure(Throwable cause) {
        cause.printStackTrace(out);
    }

    private void printProgress(int progress) {
        if (progress % step == 0) {
            out.printf(displayText, progress * 100 / reportResolution());
        }
    }
}
