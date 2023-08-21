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
package org.neo4j.internal.recordstorage;

import static java.lang.String.format;

public class InconsistentDataReadException extends RuntimeException {
    /**
     * A threshold used when reading chains of records. There's a chance that a chain contains a cycle, at which point
     * it's impossible to get to the end of the chain normally. If no cycle detection is in place then reading it
     * would never end. The idea is that readers of chains first count how many records it traverses and if crossing
     * this threshold it will start to track uniqueness among the seen record IDs and if observing non-unique IDs then
     * the conclusion is that the chain contains a cycle and must be aborted (with an exception of this type).
     *
     * The threshold itself doesn't impose any limit on chain length, it's merely a trigger to start inspecting the chain
     * more closely as it's traversed.
     */
    public static final int CYCLE_DETECTION_THRESHOLD = 100_000;

    public InconsistentDataReadException(String format, Object... parameters) {
        super(format(format, parameters));
    }

    public InconsistentDataReadException(Throwable cause, String format, Object... parameters) {
        super(format(format, parameters), cause);
    }
}
