/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;

/**
 * This interface represent a check pointer which is responsible to write check points in the transaction log.
 */
public interface CheckPointer
{
    CheckPointer NO_CHECKPOINT = new CheckPointer()
    {
        @Override
        public void checkPointIfNeeded( LogAppendEvent logAppendEvent )
        {
        }

        @Override
        public void forceCheckPoint()
        {
        }
    };

    /**
     * This method will verify that the conditions for triggering a check point hold and in such a case it will write
     * a check point in the transaction log.
     *
     * @param logAppendEvent the log append event to be used to notify the check pointing
     * @throws IOException if writing the check point fails
     */
    void checkPointIfNeeded( LogAppendEvent logAppendEvent ) throws IOException;

    /**
     * This method forces the write of a check point in the transaction log.
     *
     * It is mostly used for testing purpose and to force a check point when shutting down the database.
     *
     * @throws IOException if writing the check point fails
     */
    void forceCheckPoint() throws IOException;

    class PrintFormat
    {
        public static String prefix( long transactionId )
        {
            return "Check Pointing [" + transactionId + "]: ";
        }
    }
}
