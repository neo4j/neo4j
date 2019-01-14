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

/**
 * Group of commands to apply onto {@link StorageEngine}, as well as reference to {@link #next()} group of commands.
 * The linked list will form a batch.
 */
public interface CommandsToApply extends CommandStream
{
    /**
     * @return transaction id representing this group of commands.
     */
    long transactionId();

    /**
     * @return next group of commands in this batch.
     */
    CommandsToApply next();

    /**
     * @return {@code true} if applying this group of commands requires that any group chronologically
     * before it also needing ordering have been fully applied. This is a way to force serial application
     * of some groups and in extension their whole batches.
     */
    boolean requiresApplicationOrdering();

    /**
     * @return A string describing the contents of this batch of commands.
     */
    @Override
    String toString();
}
