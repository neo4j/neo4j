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

import org.junit.After;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TestLogPruning
{
    private GraphDatabaseAPI db;
    private FileSystemAbstraction fs;

    @After
    public void after() throws Exception
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void noPruning() throws Exception
    {
        newDb( "true" );

        for ( int i = 0; i < 100; i++ )
        {
            doTransaction();
            rotate();
            assertEquals( i+1, logCount() );
        }
    }

    @Test
    public void pruneByFileSize() throws Exception
    {
        // Given
        int size = 1050;
        newDb( size + " size" );

        doTransaction();
        rotate();
        long sizeOfOneLog = fs.getFileSize( neoDataSource()
                .getXaContainer().getLogicalLog().getFileName( 0 ) );
        int filesNeededToExceedPruneLimit = (int) Math.ceil( (double) size / (double) sizeOfOneLog );

        // When
        for ( int i = 1; i < filesNeededToExceedPruneLimit*2; i++ )
        {
            doTransaction();
            rotate();

            // Then
            assertEquals( Math.min( i+1, filesNeededToExceedPruneLimit ), logCount() );
        }
    }

    private NeoStoreXaDataSource neoDataSource()
    {
        return db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource();
    }

    @Test
    public void pruneByFileCount() throws Exception
    {
        int logsToKeep = 5;
        newDb( logsToKeep + " files" );

        for ( int i = 0; i < logsToKeep*2; i++ )
        {
            doTransaction();
            rotate();
            assertEquals( Math.min( i+1, logsToKeep ), logCount() );
        }
    }

    @Test
    public void pruneByTransactionCount() throws Exception
    {
        int transactionsToKeep = 100;
        int txsPerLog = transactionsToKeep/10;
        newDb( transactionsToKeep + " txs" );

        for ( int i = 0; i < transactionsToKeep/txsPerLog*3; i++ )
        {
            for ( int j = 0; j < txsPerLog; j++ )
            {
                doTransaction();
            }
            rotate();
            assertEquals( Math.min( i+1, transactionsToKeep/txsPerLog ), logCount() );
        }
    }

    private GraphDatabaseAPI newDb( String logPruning )
    {
        GraphDatabaseAPI db = new ImpermanentGraphDatabase( stringMap( keep_logical_logs.name(), logPruning ) )
        {
            @Override
            protected FileSystemAbstraction createFileSystemAbstraction()
            {
                return (fs = super.createFileSystemAbstraction());
            }
        };
        this.db = db;
        return db;
    }

    private void doTransaction()
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void rotate() throws Exception
    {
        neoDataSource().rotateLogicalLog();
    }

    private int logCount()
    {
        XaLogicalLog log = neoDataSource().getXaContainer().getLogicalLog();
        int count = 0;
        for ( long i = log.getHighestLogVersion()-1; i >= 0; i-- )
        {
            if ( fs.fileExists( log.getFileName( i ) ) )
            {
                count++;
            }
            else
            {
                break;
            }
        }
        return count;
    }
}
