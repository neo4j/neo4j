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
package org.neo4j.common.panic;

import org.neo4j.kernel.database.NamedDatabaseId;

@FunctionalInterface
public interface PanicEventHandler {

    @FunctionalInterface
    interface Factory {
        PanicEventHandler create(NamedDatabaseId namedDatabaseId);
    }
    /**
     * Since resources may be very limited during a panic any implementation of this interface should be as simple as possible.
     * Don't use up any extra memory or create new threads.
     */
    void onPanic(PanicReason reason, Throwable error);
}
