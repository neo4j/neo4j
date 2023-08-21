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
package org.neo4j.bolt.protocol.common.signal;

/**
 * Provides signals which control the flow of messages within the Bolt part of the network stack.
 */
public enum MessageSignal {

    /**
     * End signifies that the message has been completed successfully and should be written to the wire immediately.
     */
    END,

    /**
     * Reset signifies that an exceptional condition has occurred and that all previously encoded data shall be wiped.
     */
    RESET
}
