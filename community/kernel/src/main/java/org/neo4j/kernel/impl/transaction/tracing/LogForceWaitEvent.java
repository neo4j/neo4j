/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.tracing;

/**
 * Represents the complete process of forcing the transaction log file, after transaction commands have been written
 * to the log. This transaction might not actually end up performing the force itself, but could be included in a
 * force batch performed by another thread. This is the total force latency that this thread experienced.
 */
public interface LogForceWaitEvent extends AutoCloseable
{
    LogForceWaitEvent NULL = new LogForceWaitEvent()
    {
        @Override
        public void close()
        {
        }
    };

    /**
     * Mark the end of forcing the transaction log.
     */
    @Override
    void close();
}
