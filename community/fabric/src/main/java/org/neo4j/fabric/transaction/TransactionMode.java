/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.transaction;

/**
 * An indication of a type of a statement and what types of statement might be coming later in the same transaction.
 */
public enum TransactionMode
{
    /**
     * The current statement is a read, but a write statement might be coming later.
     */
    MAYBE_WRITE( true ),

    /**
     * The current statement is a write.
     */
    DEFINITELY_WRITE( true ),

    /**
     * The current statement is a read and no write statement will follow.
     */
    DEFINITELY_READ( false );

    private final boolean requiresWrite;

    TransactionMode( boolean requiresWrite )
    {
        this.requiresWrite = requiresWrite;
    }

    public boolean requiresWrite()
    {
        return requiresWrite;
    }
}
