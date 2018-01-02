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
package org.neo4j.index.impl.lucene;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class NonUniqueIndexTests
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void concurrentIndexPopulationAndInsertsShouldNotProduceDuplicates() throws IOException
    {
        // Given
        GraphDatabaseService db = newEmbeddedGraphDatabaseWithSlowJobScheduler();

        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( "SomeLabel" ) ).on( "key" ).create();
            tx.success();
        }
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( label( "SomeLabel" ) );
            node.setProperty( "key", "value" );
            tx.success();
        }
        db.shutdown();

        // Then
        assertThat( nodeIdsInIndex( 1, "value" ), equalTo( singletonList( node.getId() ) ) );
    }

    private GraphDatabaseService newEmbeddedGraphDatabaseWithSlowJobScheduler()
    {
        GraphDatabaseFactoryState graphDatabaseFactoryState = new GraphDatabaseFactoryState();
        graphDatabaseFactoryState.setUserLogProvider( NullLogService.getInstance().getUserLogProvider() );
        return new CommunityFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                    OperationalMode operationalMode)
            {
                return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade, operationalMode )
                {
                    @Override
                    protected Neo4jJobScheduler createJobScheduler()
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
        }.newFacade( directory.graphDbDir(), stringMap(),
                graphDatabaseFactoryState.databaseDependencies() );
    }

    private static Neo4jJobScheduler newSlowJobScheduler()
    {
        return new Neo4jJobScheduler()
        {
            @Override
            public JobHandle schedule( Group group, Runnable job )
            {
                return super.schedule( group, slowRunnable( job ) );
            }

            @Override
            public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
            {
                return super.schedule( group, slowRunnable(job), metadata );
            }
        };
    }

    private static Runnable slowRunnable( final Runnable target )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                LockSupport.parkNanos( 100_000_000 );
                target.run();
            }
        };
    }

    private List<Long> nodeIdsInIndex( int indexId, String value ) throws IOException
    {
        Config config = new Config();
        SchemaIndexProvider indexProvider = new LuceneSchemaIndexProvider( new DefaultFileSystemAbstraction(),
                DirectoryFactory.PERSISTENT, directory.graphDbDir(), NullLogProvider.getInstance(),
                new Config(), OperationalMode.single );
        IndexConfiguration indexConfig = new IndexConfiguration( false );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( indexId, indexConfig, samplingConfig );
              IndexReader reader = accessor.newReader() )
        {
            return IteratorUtil.asList( reader.seek( value ) );
        }
    }
}
