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

import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
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
    private Config config;
    private final TxIdGenerator txIdGenerator;
    private final AbstractTransactionManager txManager;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Monitors monitors;
    private final Logging logging;
    private final RecoveryVerifier recoveryVerifier;
    private final LogPruneStrategy pruneStrategy;
    private final KernelHealth kernelHealth;

    public XaFactory( Config config, TxIdGenerator txIdGenerator, AbstractTransactionManager txManager,
                      FileSystemAbstraction fileSystemAbstraction,
                      Monitors monitors, Logging logging, RecoveryVerifier recoveryVerifier,
                      LogPruneStrategy pruneStrategy, KernelHealth kernelHealth )
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

    public XaContainer newXaContainer( XaDataSource xaDataSource, File logicalLog, XaCommandFactory cf,
                                       InjectedTransactionValidator injectedTxValidator, XaTransactionFactory tf,
                                       TransactionStateFactory stateFactory, TransactionInterceptorProviders providers,
                                       boolean readOnly )
    {
        if ( logicalLog == null || cf == null || tf == null )
        {
            throw new IllegalArgumentException( "Null parameter, "
                    + "LogicalLog[" + logicalLog + "] CommandFactory[" + cf
                    + "TransactionFactory[" + tf + "]" );
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
        else if ( providers.shouldInterceptDeserialized() && providers.hasAnyInterceptorConfigured() )
        {
            log = new InterceptingXaLogicalLog( logicalLog, rm, cf, tf, providers, monitors, fileSystemAbstraction,
                logging, pruneStrategy, stateFactory, kernelHealth, rotateAtSize, injectedTxValidator );
        }
        else
        {
            log = new XaLogicalLog( logicalLog, rm, cf, tf, fileSystemAbstraction,
                    monitors, logging, pruneStrategy, stateFactory, kernelHealth, rotateAtSize, injectedTxValidator );
        }

        // TODO These setters should be removed somehow
        rm.setLogicalLog( log );
        tf.setLogicalLog( log );

        return new XaContainer(rm, log);
    }
}
