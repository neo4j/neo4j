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
package org.neo4j.packstream.error.struct;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;

public class IllegalStructSizeException extends PackstreamStructException {
    private final long expectedMin;
    private final long expectedMax;
    private final long actual;

    public IllegalStructSizeException(long expected, long actual) {
        super("Illegal struct size: Expected struct to be " + expected + " fields but got " + actual);
        this.expectedMin = expected;
        this.expectedMax = expected;
        this.actual = actual;
    }

    public IllegalStructSizeException(ErrorGqlStatusObject gqlStatusObject, long expected, long actual) {
        super(gqlStatusObject, "Illegal struct size: Expected struct to be " + expected + " fields but got " + actual);

        this.expectedMin = expected;
        this.expectedMax = expected;
        this.actual = actual;
    }

    public long getExpectedMin() {
        return this.expectedMin;
    }

    public long getExpectedMax() {
        return this.expectedMax;
    }

    public long getActual() {
        return this.actual;
    }
}
