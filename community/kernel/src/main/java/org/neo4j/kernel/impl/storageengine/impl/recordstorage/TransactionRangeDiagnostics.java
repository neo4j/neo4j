/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.io.IOException;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.logging.Logger;

public class TransactionRangeDiagnostics implements DiagnosticsExtractor<NeoStores>
{
    private final PhysicalLogFiles logFiles;

    public TransactionRangeDiagnostics( PhysicalLogFiles logFiles )
    {
        this.logFiles = logFiles;
    }

    @Override
    public void dumpDiagnostics( NeoStores source, DiagnosticsPhase phase, Logger logger )
    {
        try
        {
            for ( long logVersion = logFiles.getLowestLogVersion();
                    logFiles.versionExists( logVersion ); logVersion++ )
            {
                if ( logFiles.hasAnyTransaction( logVersion ) )
                {
                    LogHeader header = logFiles.extractHeader( logVersion );
                    long firstTransactionIdInThisLog = header.lastCommittedTxId + 1;
                    logger.log( "Oldest transaction " + firstTransactionIdInThisLog +
                            " found in log with version " + logVersion );
                    return;
                }
            }
            logger.log( "No transactions found in any log" );
        }
        catch ( IOException e )
        {   // It's fine, we just tried to be nice and log this. Failing is OK
            logger.log( "Error trying to figure out oldest transaction in log" );
        }
    }
}
