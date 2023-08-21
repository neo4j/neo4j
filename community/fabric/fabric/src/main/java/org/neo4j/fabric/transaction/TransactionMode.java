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
package org.neo4j.fabric.transaction;

import org.neo4j.bolt.protocol.common.message.AccessMode;

/**
 * An indication of a type of a statement and what types of statement might be coming later in the same transaction.
 */
public enum TransactionMode {
    /**
     * The current statement is a read, but a write statement might be coming later.
     */
    MAYBE_WRITE(AccessMode.WRITE),

    /**
     * The current statement is a write.
     */
    DEFINITELY_WRITE(AccessMode.WRITE),

    /**
     * The current statement is a read and no write statement will follow.
     */
    DEFINITELY_READ(AccessMode.READ);

    private final AccessMode concreteAccessMode;

    TransactionMode(AccessMode concreteAccessMode) {
        this.concreteAccessMode = concreteAccessMode;
    }

    public boolean requiresWrite() {
        return concreteAccessMode == AccessMode.WRITE;
    }

    public AccessMode concreteAccessMode() {
        return concreteAccessMode;
    }
}
