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
package org.neo4j.kernel;

import java.io.IOException;

import org.neo4j.internal.diagnostics.DiagnosticsExtractor;
import org.neo4j.internal.diagnostics.DiagnosticsPhase;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Logger;

enum DataSourceDiagnostics implements DiagnosticsExtractor<NeoStoreDataSource>
{
    TRANSACTION_RANGE( "Transaction log:" )
            {
                @Override
                void dump( NeoStoreDataSource source, Logger log )
                {
                    LogFiles logFiles = source.getDependencyResolver().resolveDependency( LogFiles.class );
                    try
                    {
                        for ( long logVersion = logFiles.getLowestLogVersion();
                              logFiles.versionExists( logVersion ); logVersion++ )
                        {
                            if ( logFiles.hasAnyEntries( logVersion ) )
                            {
                                LogHeader header = logFiles.extractHeader( logVersion );
                                long firstTransactionIdInThisLog = header.lastCommittedTxId + 1;
                                log.log( "Oldest transaction " + firstTransactionIdInThisLog +
                                         " found in log with version " + logVersion );
                                return;
                            }
                        }
                        log.log( "No transactions found in any log" );
                    }
                    catch ( IOException e )
                    {   // It's fine, we just tried to be nice and log this. Failing is OK
                        log.log( "Error trying to figure out oldest transaction in log" );
                    }
                }
            };

    private final String message;

    DataSourceDiagnostics( String message )
    {
        this.message = message;
    }

    @Override
    public void dumpDiagnostics( final NeoStoreDataSource source, DiagnosticsPhase phase, Logger logger )
    {
        if ( applicable( phase ) )
        {
            logger.log( message );
            dump( source, logger );
        }
    }

    boolean applicable( DiagnosticsPhase phase )
    {
        return phase.isInitialization() || phase.isExplicitlyRequested();
    }

    abstract void dump( NeoStoreDataSource source, Logger logger );
}
