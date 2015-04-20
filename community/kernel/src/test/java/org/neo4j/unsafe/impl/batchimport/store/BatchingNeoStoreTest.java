/*
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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;
import static org.neo4j.unsafe.impl.batchimport.store.io.Monitor.NO_MONITOR;

public class BatchingNeoStoreTest
{
    @Test
    public void shouldNotOpenStoreWithNodesOrRelationshipsInIt() throws Exception
    {
        // GIVEN
        someDataInTheDatabase();

        // WHEN
        PageCache pageCache = new BatchingPageCache( fsr.get(), 10_000, 1, SYNCHRONOUS, NO_MONITOR );
        try
        {
            new BatchingNeoStore( fsr.get(), storeDir, DEFAULT, NO_MONITOR,
                    new DevNullLoggingService(), new Monitors(), SYNCHRONOUS, EMPTY );
            fail( "Should fail on existing data" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "already contains" ) );
        }
        finally
        {
            pageCache.close();
        }
    }

    private void someDataInTheDatabase()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fsr.get() )
                .newImpermanentDatabase( storeDir.getAbsolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final File storeDir = new File( "dir" );
}
