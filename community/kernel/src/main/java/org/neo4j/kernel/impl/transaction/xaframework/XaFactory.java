/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.StringLogger;

/**
* TODO
*/
public class XaFactory
{
    private Map<String, String> config;
    private TxIdGenerator txIdGenerator;
    private AbstractTransactionManager txManager;
    private LogBufferFactory logBufferFactory;
    private FileSystemAbstraction fileSystemAbstraction;
    private StringLogger stringLogger;
    private final RecoveryVerifier recoveryVerifier;

    public XaFactory(Map<String, String> config, TxIdGenerator txIdGenerator, AbstractTransactionManager txManager,
            LogBufferFactory logBufferFactory, FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger, RecoveryVerifier recoveryVerifier )
    {
        this.config = config;
        this.txIdGenerator = txIdGenerator;
        this.txManager = txManager;
        this.logBufferFactory = logBufferFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
        this.recoveryVerifier = recoveryVerifier;
    }

    public XaContainer newXaContainer(XaDataSource xaDataSource, String logicalLog, XaCommandFactory cf, XaTransactionFactory tf,
                                      TransactionInterceptor interceptors)
    {
        if ( logicalLog == null || cf == null || tf == null )
        {
            throw new IllegalArgumentException( "Null parameter, "
                    + "LogicalLog[" + logicalLog + "] CommandFactory[" + cf
                    + "TransactionFactory[" + tf + "]" );
        }

        // TODO The dependencies between XaRM, LogicalLog and XaTF should be resolved to avoid the setter
        XaResourceManager rm = new XaResourceManager( xaDataSource, tf, txIdGenerator, txManager, recoveryVerifier, logicalLog );

        XaLogicalLog log;
        if ( "true".equalsIgnoreCase( (String) config.get(Config.INTERCEPT_DESERIALIZED_TRANSACTIONS) )
                && interceptors != null )
        {
            log = new InterceptingXaLogicalLog( logicalLog, rm, cf, tf, interceptors, logBufferFactory, fileSystemAbstraction, stringLogger );
        }
        else
        {
            log = new XaLogicalLog( logicalLog, rm, cf, tf, logBufferFactory, fileSystemAbstraction, stringLogger );
        }

        // TODO These setters should be removed somehow
        rm.setLogicalLog( log );
        tf.setLogicalLog( log );

        return new XaContainer(rm, log);
    }
}
