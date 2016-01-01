/**
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriterFactory;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_log_rotation_threshold;

/**
* TODO
*/
public class XaFactory
{
    private final Config config;
    private final TxIdGenerator txIdGenerator;
    private final AbstractTransactionManager txManager;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Monitors monitors;
    private final Logging logging;
    private final RecoveryVerifier recoveryVerifier;
    private final LogPruneStrategy pruneStrategy;
    private final KernelHealth kernelHealth;

    public XaFactory( Config config, TxIdGenerator txIdGenerator, AbstractTransactionManager txManager,
                      FileSystemAbstraction fileSystemAbstraction, Monitors monitors, Logging logging,
                      RecoveryVerifier recoveryVerifier, LogPruneStrategy pruneStrategy, KernelHealth kernelHealth )
    {
        this.config = config;
        this.txIdGenerator = txIdGenerator;
        this.txManager = txManager;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.monitors = monitors;
        this.logging = logging;
        this.recoveryVerifier = recoveryVerifier;
        this.pruneStrategy = pruneStrategy;
        this.kernelHealth = kernelHealth;
    }

    public XaContainer newXaContainer( final XaDataSource xaDataSource, File logicalLog,
                                       XaCommandReaderFactory commandReaderFactory,
                                       XaCommandWriterFactory commandWriterFactory,
                                       InjectedTransactionValidator injectedTxValidator,
                                       XaTransactionFactory tf, TransactionStateFactory stateFactory,
                                       TransactionInterceptorProviders providers, boolean readOnly, Function<List
            <LogEntry>, List<LogEntry>> transactionTranslator )
    {
        if ( logicalLog == null || commandReaderFactory == null || commandWriterFactory == null || tf == null )
        {
            throw new IllegalArgumentException( "Null parameter, "
                    + "LogicalLog[" + logicalLog + "], CommandReaderFactory[" + commandReaderFactory
                    + "CommandWriterFactory[" + commandWriterFactory
                    + "], TransactionFactory[" + tf + "]" );
        }

        // TODO The dependencies between XaRM, LogicalLog and XaTF should be resolved to avoid the setter
        XaResourceManager rm = new XaResourceManager( xaDataSource, tf, txIdGenerator, txManager, recoveryVerifier,
                logicalLog.getName(), monitors );

        long rotateAtSize = config.get( logical_log_rotation_threshold );
        XaLogicalLog log;
        if( readOnly)
        {
            log = new NoOpLogicalLog( logging );
        }
        else
        {
            // Note: The interceptor and the transactionTranslator serve very similar purposes. Both take input
            // log entries and act on those as part of the commit path. The only distinction is that interceptors
            // can reject transactions, while the translator translates transactions from old formats to a new format.
            // They should probably be consolidated into one thing.
            Function<List<LogEntry>, List<LogEntry>> interceptor = null;
            if ( providers.shouldInterceptDeserialized() && providers.hasAnyInterceptorConfigured() )
            {
                interceptor = new LogEntryVisitorAdapter( providers, xaDataSource );
            }
            else
            {
                interceptor = Functions.identity();
            }
            log = new XaLogicalLog( logicalLog, rm, commandReaderFactory, commandWriterFactory, tf, fileSystemAbstraction,
                    monitors, logging, pruneStrategy, stateFactory, kernelHealth, rotateAtSize, injectedTxValidator,
                    interceptor, transactionTranslator );
        }

        // TODO These setters should be removed somehow
        rm.setLogicalLog( log );
        tf.setLogicalLog( log );

        return new XaContainer(rm, log);
    }
}
