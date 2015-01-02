/**
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

public class InterceptingXaLogicalLog extends XaLogicalLog
{
    private final XaDataSource ds;
    private final TransactionInterceptorProviders providers;

    public InterceptingXaLogicalLog( File fileName, XaResourceManager xaRm,
                                     XaCommandFactory cf, XaTransactionFactory xaTf,
                                     TransactionInterceptorProviders providers,
                                     Monitors monitors, FileSystemAbstraction fileSystem, Logging logging,
                                     LogPruneStrategy pruneStrategy, TransactionStateFactory stateFactory,
                                     KernelHealth kernelHealth, long rotateAtSize, InjectedTransactionValidator injectedTxValidator )
    {
        super( fileName, xaRm, cf, xaTf, fileSystem, monitors, logging, pruneStrategy,
                stateFactory, kernelHealth, rotateAtSize, injectedTxValidator );
        this.providers = providers;
        this.ds = xaRm.getDataSource();
    }

    @Override
    protected LogDeserializer getLogDeserializer( ReadableByteChannel byteChannel )
    {
        // This is created every time because transaction interceptors can be stateful
        final TransactionInterceptor interceptor = providers.resolveChain( ds );

        LogDeserializer toReturn = new LogDeserializer( byteChannel, bufferMonitor )
        {
            @Override
            protected void intercept( List<LogEntry> entries )
            {
                for ( LogEntry entry : entries )
                {
                    if ( entry instanceof LogEntry.Command )
                    {
                        LogEntry.Command commandEntry = (LogEntry.Command) entry;
                        if ( commandEntry.getXaCommand() instanceof Command )
                        {
                            ( (Command) commandEntry.getXaCommand() ).accept( interceptor );
                        }
                    }
                    else if ( entry instanceof LogEntry.Start )
                    {
                        interceptor.setStartEntry( (LogEntry.Start) entry );
                    }
                    else if ( entry instanceof LogEntry.Commit )
                    {
                        interceptor.setCommitEntry( (LogEntry.Commit) entry );
                    }
                }
                interceptor.complete();
            }
        };
        return toReturn;
    }
}
