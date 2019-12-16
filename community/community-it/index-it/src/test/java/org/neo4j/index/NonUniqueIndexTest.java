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
package org.neo4j.index;

import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.LockSupport;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;

@PageCacheExtension
class NonUniqueIndexTest
{
    private static final String LABEL = "SomeLabel";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String INDEX_NAME = "indexName";

    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @Test
    void concurrentIndexPopulationAndInsertsShouldNotProduceDuplicates() throws Exception
    {
        // Given
        GraphDatabaseService db = newEmbeddedGraphDatabaseWithSlowJobScheduler();
        try
        {
            // When
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().indexFor( label( LABEL ) ).on( KEY ).withName( INDEX_NAME ).create();
                tx.commit();
            }
            Node node;
            try ( Transaction tx = db.beginTx() )
            {
                node = tx.createNode( label( LABEL ) );
                node.setProperty( KEY, VALUE );
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 1, MINUTES );
                tx.commit();
            }

            // Then
            try ( Transaction tx = db.beginTx() )
            {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                IndexDescriptor index = ktx.schemaRead().indexGetForName( INDEX_NAME );
                IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
                try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
                {
                    ktx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false,
                            IndexQuery.exact( 1, VALUE ) );
                    assertTrue( cursor.next() );
                    assertEquals( node.getId(), cursor.nodeReference() );
                    assertFalse( cursor.next() );
                }
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private GraphDatabaseService newEmbeddedGraphDatabaseWithSlowJobScheduler()
    {
        // Inject JobScheduler
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( createJobScheduler() );

        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setExternalDependencies( dependencies )
                .build();

        return managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private static CentralJobScheduler createJobScheduler()
    {
        CentralJobScheduler scheduler = newSlowJobScheduler();
        scheduler.init();
        return scheduler;
    }

    private static CentralJobScheduler newSlowJobScheduler()
    {
        return new CentralJobScheduler( Clocks.nanoClock() )
        {
            @Override
            public JobHandle<?> schedule( Group group, Runnable job )
            {
                return super.schedule( group, slowRunnable( job ) );
            }
        };
    }

    private static Runnable slowRunnable( final Runnable target )
    {
        return () ->
        {
            LockSupport.parkNanos( 100_000_000L );
            target.run();
        };
    }
}
