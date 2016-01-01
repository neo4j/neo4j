/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1.writeLogHeader;

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

    public void ensureAtLeastOneLogicalLogPresent( String sourceHostNameOrIp, int sourcePort, GraphDatabaseAPI targetDb )
    {
        // Then go over all datasources, try to extract the latest tx
        Set<String> noTxPresent = new HashSet<>();
        XaDataSourceManager dsManager = targetDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class );
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            long lastTx = ds.getLastCommittedTxId();
            try
            {
                // This fails if the tx is not present with NSLVE
                ds.getMasterForCommittedTx( lastTx );
            }
            catch ( NoSuchLogVersionException e )
            {
                // Note the name of the datasource
                noTxPresent.add( ds.getName() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        if ( !noTxPresent.isEmpty() )
        {
                    /*
                     * Create a fake slave context, asking for the transactions that
                     * span the next-to-last up to the latest for each datasource
                     */
            BackupClient recoveryClient = new BackupClient(
                    sourceHostNameOrIp, sourcePort,
                    targetDb.getDependencyResolver().resolveDependency( Logging.class ),
                    targetDb.getDependencyResolver().resolveDependency( Monitors.class ),
                    targetDb.storeId() );
            recoveryClient.start();
            Response<Void> recoveryResponse = null;
            Map<String, Long> recoveryDiff = new HashMap<>();
            for ( String ds : noTxPresent )
            {
                recoveryDiff.put( ds, -1L );
            }
            RequestContext recoveryCtx = addDiffToSlaveContext( slaveContextOf( dsManager ), recoveryDiff );
            try
            {
                recoveryResponse = recoveryClient.incrementalBackup( recoveryCtx );
                // Ok, the response is here, apply it.
                TransactionStream txs = recoveryResponse.transactions();
                ByteBuffer scratch = ByteBuffer.allocate( 64 );
                while ( txs.hasNext() )
                {
                            /*
                             * For each tx stream in the response, create the latest archived
                             * logical log file and write out in there the transaction.
                             *
                             */
                    Triplet<String, Long, TxExtractor> tx = txs.next();
                    scratch.clear();
                    XaDataSource ds = dsManager.getXaDataSource( tx.first() );
                    long logVersion = ds.getCurrentLogVersion() - 1;
                    FileChannel newLog = new RandomAccessFile( ds.getFileName( logVersion ), "rw" ).getChannel();
                    newLog.truncate( 0 );
                    writeLogHeader( scratch, logVersion, tx.second() - 1 );
                    // scratch buffer is flipped by writeLogHeader
                    newLog.write( scratch );
                    ReadableByteChannel received = tx.third().extract();
                    scratch.flip();
                    while ( received.read( scratch ) > 0 )
                    {
                        scratch.flip();
                        newLog.write( scratch );
                        scratch.flip();
                    }
                    newLog.force( false );
                    newLog.close();
                    received.close();
                }
            }
            catch( RuntimeException e)
            {
                if(e.getCause() != null && e.getCause() instanceof NoSuchLogVersionException)
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
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( recoveryResponse != null )
                {
                    recoveryResponse.close();
                }
                try
                {
                    recoveryClient.stop();
                }
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
                targetDb.shutdown();
            }
        }
    }

    private RequestContext slaveContextOf( XaDataSourceManager dsManager )
    {
        List<RequestContext.Tx> txs = new ArrayList<>();
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            txs.add( RequestContext.lastAppliedTx( ds.getName(), ds.getLastCommittedTxId() ) );
        }
        return RequestContext.anonymous( txs.toArray( new RequestContext.Tx[txs.size()] ) );
    }

    private RequestContext addDiffToSlaveContext( RequestContext original,
                                                  Map<String, Long> diffPerDataSource )
    {
        RequestContext.Tx[] oldTxs = original.lastAppliedTransactions();
        RequestContext.Tx[] newTxs = new RequestContext.Tx[oldTxs.length];
        for ( int i = 0; i < oldTxs.length; i++ )
        {
            RequestContext.Tx oldTx = oldTxs[i];
            String dsName = oldTx.getDataSourceName();
            long originalTxId = oldTx.getTxId();
            Long diff = diffPerDataSource.get( dsName );
            if ( diff == null )
            {
                diff = 0L;
            }
            long newTxId = originalTxId + diff;
            newTxs[i] = RequestContext.lastAppliedTx( dsName, newTxId );
        }
        return RequestContext.anonymous( newTxs );
    }

}
