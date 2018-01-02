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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

public interface LogFileInformation
{
    /**
     * @return the reachable transaction that is farthest back of them all, in any existing version.
     */
    long getFirstExistingTxId() throws IOException;

    /**
     * @param version the log version to get first committed tx for.
     * @return the first committed transaction id for the log with {@code version}.
     * If that log doesn't exist -1 is returned.
     */
    long getFirstCommittedTxId( long version ) throws IOException;

    /**
     * @return the last committed transaction id for this Log
     */
    long getLastCommittedTxId();

    /**
     * @param version the log version to get first tx timestamp for.
     * @return the timestamp for the start record for the first encountered transaction
     * in the log {@code version}.
     */
    long getFirstStartRecordTimestamp( long version ) throws IOException;
}
