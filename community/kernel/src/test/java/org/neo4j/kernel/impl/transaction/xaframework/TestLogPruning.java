/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestLogPruning
{
    private GraphDatabaseAPI db;
    private FileSystemAbstraction fs;
    private PhysicalLogFiles files;

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
        long sizeOfOneLog = fs.getFileSize( files.getHistoryFileName( 0 ) );
        int filesNeededToExceedPruneLimit = (int) Math.ceil( (double) size / (double) sizeOfOneLog );

        // When
        for ( int i = 1; i < filesNeededToExceedPruneLimit*2; i++ )
        {
            doTransaction();

            // Then
            assertEquals( Math.min( i+1, filesNeededToExceedPruneLimit ), logCount() );
        }
    }

    @Test
    public void pruneByFileCount() throws Exception
    {
        int logsToKeep = 5;
        newDb( logsToKeep + " files" );

        for ( int i = 0; i < logsToKeep*2; i++ )
        {
            doTransaction();
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
            assertEquals( Math.min( i+1, transactionsToKeep/txsPerLog ), logCount() );
        }
    }

    private GraphDatabaseAPI newDb( String logPruning )
    {
        fs = new EphemeralFileSystemAbstraction();
        GraphDatabaseAPI db = (GraphDatabaseAPI) new ImpermanentGraphDatabase( stringMap( keep_logical_logs.name(), logPruning ) )
        {
            @Override
            protected FileSystemAbstraction createFileSystemAbstraction()
            {
                return fs;
            }
        };
        this.db = db;
        files = new PhysicalLogFiles( new File(db.getStoreDir()), GraphDatabaseSettings.logical_log.getDefaultValue(), fs );
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

    private int logCount()
    {
        int count = 0;
        for ( long i = files.getHighestHistoryLogVersion(); i >= 0; i-- )
        {
            if ( fs.fileExists( files.getHistoryFileName( i ) ) )
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
