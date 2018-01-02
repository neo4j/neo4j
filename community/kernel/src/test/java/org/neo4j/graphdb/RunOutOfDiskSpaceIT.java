/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.test.LimitedFileSystemGraphDatabase;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RunOutOfDiskSpaceIT
{
    public final @Rule CleanupRule cleanup = new CleanupRule();

    @Test
    public void shouldPropagateIOExceptions() throws Exception
    {
        // Given
        TransactionFailureException exceptionThrown = null;

        String storeDir = testDirectory.absolutePath();
        LimitedFileSystemGraphDatabase db = new LimitedFileSystemGraphDatabase( storeDir );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        long logVersion = db.getDependencyResolver().resolveDependency( NeoStores.class ).getMetaDataStore()
                            .getCurrentLogVersion();

        db.runOutOfDiskSpaceNao();

        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        catch ( TransactionFailureException e )
        {
            exceptionThrown = e;
        }
        finally
        {
            Assert.assertNotNull( "Expected tx finish to throw TransactionFailureException when filesystem is full.",
                    exceptionThrown );
            assertTrue( Exceptions.contains( exceptionThrown, IOException.class ) );
        }

        db.somehowGainMoreDiskSpace(); // to help shutting down the db
        db.shutdown();

        PageCache pageCache = pageCacheRule.getPageCache( db.getFileSystem() );
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        assertEquals( logVersion, MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.LOG_VERSION ) );
    }

    @Test
    public void shouldStopDatabaseWhenOutOfDiskSpace() throws Exception
    {
        // Given
        TransactionFailureException expectedCommitException = null;
        TransactionFailureException expectedStartException = null;
        String storeDir = testDirectory.absolutePath();
        LimitedFileSystemGraphDatabase db = cleanup.add( new LimitedFileSystemGraphDatabase( storeDir ) );


        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        long logVersion = db.getDependencyResolver().resolveDependency( NeoStores.class ).getMetaDataStore()
                            .getCurrentLogVersion();

        db.runOutOfDiskSpaceNao();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        catch ( TransactionFailureException e )
        {
            expectedCommitException = e;
        }
        finally
        {
            Assert.assertNotNull( "Expected tx finish to throw TransactionFailureException when filesystem is full.",
                    expectedCommitException );
        }

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            fail( "Expected tx begin to throw TransactionFailureException when tx manager breaks." );
        }
        catch ( TransactionFailureException e )
        {
            expectedStartException = e;
        }
        finally
        {
            Assert.assertNotNull( "Expected tx begin to throw TransactionFailureException when tx manager breaks.",
                    expectedStartException );
        }

        // Then
        db.somehowGainMoreDiskSpace(); // to help shutting down the db
        db.shutdown();

        PageCache pageCache = pageCacheRule.getPageCache( db.getFileSystem() );
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        assertEquals( logVersion, MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.LOG_VERSION ) );
    }

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
}
