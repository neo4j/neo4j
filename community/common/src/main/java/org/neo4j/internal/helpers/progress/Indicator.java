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

public abstract class Indicator {
    public static final Indicator NONE = new Indicator(1) {
        @Override
        protected void progress(int from, int to) {}
    };

    private final int reportResolution;

    public Indicator(int reportResolution) {
        this.reportResolution = reportResolution;
    }

    protected abstract void progress(int from, int to);

    public void mark(char mark) {}

    int reportResolution() {
        return reportResolution;
    }

    public void startProcess(long totalCount) {}

    public void failure(Throwable cause) {}
}
