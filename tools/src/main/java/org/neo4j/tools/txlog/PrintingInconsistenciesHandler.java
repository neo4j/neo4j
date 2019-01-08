/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.txlog;

import java.io.PrintStream;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Handler that simply prints given number of inconsistencies to {@link PrintingInconsistenciesHandler#out} and throws
 * an exception if too many inconsistencies are found.
 */
class PrintingInconsistenciesHandler implements InconsistenciesHandler
{
    private static final int DEFAULT_NUMBER_OF_INCONSISTENCIES_TO_PRINT = 1024;

    private final PrintStream out;

    private int seenInconsistencies;

    PrintingInconsistenciesHandler( PrintStream out )
    {
        this.out = out;
    }

    @Override
    public void reportInconsistentCheckPoint( long logVersion, LogPosition logPosition, long size )
    {
        out.println( "Inconsistent check point found in log with version " + logVersion );
        long pointedLogVersion = logPosition.getLogVersion();
        out.println( "\tCheck point claims to recover from " + logPosition.getByteOffset() + " in log with version " + pointedLogVersion );
        if ( size >= 0 )
        {
            out.println( "\tLog with version " + pointedLogVersion + " has size " + size );
        }
        else
        {
            out.println( "\tLog with version " + pointedLogVersion + " does not exist" );
        }
        incrementAndPerhapsThrow();
    }

    @Override
    public void reportInconsistentCommand( RecordInfo<?> committed, RecordInfo<?> current )
    {
        out.println( "Inconsistent after and before states:" );
        out.println( "\t+" + committed );
        out.println( "\t-" + current );
        incrementAndPerhapsThrow();
    }

    @Override
    public void reportInconsistentTxIdSequence( long lastSeenTxId, long currentTxId )
    {
        out.printf( "Inconsistent in tx id sequence between transactions %d and %d %n", lastSeenTxId, currentTxId );
        incrementAndPerhapsThrow();
    }

    private void incrementAndPerhapsThrow()
    {
        seenInconsistencies++;
        if ( seenInconsistencies >= DEFAULT_NUMBER_OF_INCONSISTENCIES_TO_PRINT )
        {
            throw new RuntimeException( "Too many inconsistencies found" );
        }
    }
}
