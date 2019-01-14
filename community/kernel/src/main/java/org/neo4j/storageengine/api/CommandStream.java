/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;

/**
 * A stream of commands from one or more transactions, that can be serialized to a transaction log or applied to a
 * store.
 */
public interface CommandStream
{
    /**
     * Accepts a visitor into the commands making up this transaction.
     * @param visitor {@link Visitor} which will see the commands.
     * @return {@code true} if any {@link StorageCommand} visited returned {@code true}, otherwise {@code false}.
     * @throws IOException if there were any problem reading the commands.
     */
    boolean accept( Visitor<StorageCommand,IOException> visitor ) throws IOException;
}
