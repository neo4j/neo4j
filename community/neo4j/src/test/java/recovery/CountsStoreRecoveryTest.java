/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package recovery;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class CountsStoreRecoveryTest
{
    @Test
    public void shouldRecoverTheCountsStoreEvenWhenIfNeoStoreDoesNotNeedRecovery() throws Exception
    {
        // given
        createNode( "A" );
        checkPoint();
        createNode( "B" );
        flushNeoStoreOnly();

        // when
        crashAndRestart();

        // then
        final AtomicInteger number = new AtomicInteger( 0 );
        counts().accept( new CountsVisitor.Adapter()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                number.incrementAndGet();
                if ( labelId != -1 )
                {
                    assertEquals( 1, count );
                }
                else
                {
                    assertEquals( 2, count );
                }
            }
        } );
        assertEquals( 3, number.get() );
    }

    private void flushNeoStoreOnly()
    {
        NeoStores neoStores = ((GraphDatabaseAPI) db).getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();
        metaDataStore.flush();
    }

    private CountsTracker counts()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores()
                                      .getCounts();
    }

    private void checkPoint() throws IOException
    {
        ((GraphDatabaseAPI) db).getDependencyResolver()
                               .resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    private void crashAndRestart() throws Exception
    {
        final GraphDatabaseService db1 = db;
        FileSystemAbstraction uncleanFs = fsRule.snapshot( db1::shutdown );
        db = databaseFactory( uncleanFs, indexProvider ).newImpermanentDatabase();
    }

    private void createNode( String label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( label ) );

            tx.success();
        }
    }

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private GraphDatabaseService db;
    private final InMemoryIndexProvider indexProvider = new InMemoryIndexProvider( 100 );

    @Before
    public void before()
    {
        db = databaseFactory( fsRule.get(), indexProvider ).newImpermanentDatabase();
    }

    private TestGraphDatabaseFactory databaseFactory( FileSystemAbstraction fs, InMemoryIndexProvider indexProvider )
    {
        return new TestGraphDatabaseFactory()
                .setFileSystem( fs ).setKernelExtensions( asList( new InMemoryIndexProviderFactory( indexProvider ) ) );
    }

    @After
    public void after()
    {
        db.shutdown();
    }
}
