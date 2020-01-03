/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public class NonUniqueIndexTest
{
    private static final String LABEL = "SomeLabel";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    @Rule
    public PageCacheAndDependenciesRule resources = new PageCacheAndDependenciesRule().with( new DefaultFileSystemRule() );

    @Test
    public void concurrentIndexPopulationAndInsertsShouldNotProduceDuplicates() throws Exception
    {
        // Given
        Config config = Config.defaults();
        GraphDatabaseService db = newEmbeddedGraphDatabaseWithSlowJobScheduler( config );
        try
        {
            // When
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label( LABEL ) ).on( KEY ).create();
                tx.success();
            }
            Node node;
            try ( Transaction tx = db.beginTx() )
            {
                node = db.createNode( label( LABEL ) );
                node.setProperty( KEY, VALUE );
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, MINUTES );
                tx.success();
            }

            // Then
            try ( Transaction tx = db.beginTx() )
            {
                KernelTransaction ktx = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(
                        ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
                IndexReference index = ktx.schemaRead().index( ktx.tokenRead().nodeLabel( LABEL ), ktx.tokenRead().propertyKey( KEY ) );
                NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor();
                ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, IndexQuery.exact( 1, VALUE ) );
                assertTrue( cursor.next() );
                assertEquals( node.getId(), cursor.nodeReference() );
                assertFalse( cursor.next() );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService newEmbeddedGraphDatabaseWithSlowJobScheduler( Config config )
    {
        GraphDatabaseFactoryState graphDatabaseFactoryState = new GraphDatabaseFactoryState();
        graphDatabaseFactoryState.setUserLogProvider( NullLogService.getInstance().getUserLogProvider() );
        return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
            {
                return new PlatformModule( storeDir, config, databaseInfo, dependencies )
                {
                    @Override
                    protected CentralJobScheduler createJobScheduler()
                    {
                        return newSlowJobScheduler();
                    }

                    @Override
                    protected LogService createLogService( LogProvider userLogProvider )
                    {
                        return NullLogService.getInstance();
                    }
                };
            }
        }.newFacade( resources.directory().storeDir(), config,
                graphDatabaseFactoryState.databaseDependencies() );
    }

    private static CentralJobScheduler newSlowJobScheduler()
    {
        return new CentralJobScheduler()
        {
            @Override
            public JobHandle schedule( Group group, Runnable job )
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
