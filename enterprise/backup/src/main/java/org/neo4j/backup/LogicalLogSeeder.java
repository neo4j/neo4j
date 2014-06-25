/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.IOException;

import org.neo4j.com.Response;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppender;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.com.RequestContext.anonymous;

/**
 * When performing a full backup, if there are no transactions to apply after the backup, we will not have any logical
 * log files on the client side. This is bad, because those are needed to perform incremental backup later on, so we
 * need to explicitly create "seed" logical log files. These would contain only a single transaction, the latest one
 * performed. This class owns that process.
 */
public class LogicalLogSeeder
{
    private final StringLogger logger;

    public LogicalLogSeeder( StringLogger logger )
    {
        this.logger = logger;
    }

    public void ensureAtLeastOneLogicalLogPresent( String sourceHostNameOrIp, int sourcePort,
            GraphDatabaseAPI targetDb ) throws IOException
    {
        // Try to extract metadata about the last transaction
        DependencyResolver resolver = targetDb.getDependencyResolver();
        TransactionIdStore transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
        LogicalTransactionStore transactionStore = resolver.resolveDependency( LogicalTransactionStore.class );
        try
        {
            transactionStore.getMetadataFor( transactionIdStore.getLastCommittingTransactionId() );
            return; // since we have transaction metadata for the last transaction.
        }
        catch ( NoSuchTransactionException e )
        {   // we need to fetch transaction metadata, below.
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        // Create a fake slave context, asking for the transactions that span the next-to-last up to the last
        LifeSupport life = new LifeSupport();
        BackupClient recoveryClient = life.add( new BackupClient(
                sourceHostNameOrIp, sourcePort,
                resolver.resolveDependency( Logging.class ),
                resolver.resolveDependency( Monitors.class ),
                targetDb.storeId() ) );
        life.start();
        try ( Response<Void> response = recoveryClient.incrementalBackup(
                anonymous( transactionIdStore.getLastCommittingTransactionId()-1 ) ) )
        {
            TransactionAppender appender = transactionStore.getAppender();
            for ( CommittedTransactionRepresentation transaction : response.getTxs() )
            {
                appender.append( transaction );
            }
        }
        catch ( RuntimeException e )
        {
            if ( e.getCause() != null && e.getCause() instanceof MissingLogDataException )
            {
                logger.warn( "Important: There are no available transaction logs on the target database, which " +
                        "means the backup could not save a point-in-time reference. This means you cannot use this " +
                        "backup for incremental backups, and it means you cannot use it directly to seed an HA " +
                        "cluster. The next time you perform a backup, a full backup will be done. If you wish to " +
                        "use this backup as a seed for a cluster, you need to start a stand-alone database on " +
                        "it, and commit one write transaction, to create the transaction log needed to seed the " +
                        "cluster. To avoid this happening, make sure you never manually delete transaction log " +
                        "files (nioneo_logical.log.vXXX), and that you configure the database to keep at least a " +
                        "few days worth of transaction logs." );
            }
        }
        finally
        {
            life.shutdown();
        }
    }
}
