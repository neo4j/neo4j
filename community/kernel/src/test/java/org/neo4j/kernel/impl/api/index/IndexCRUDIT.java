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
package org.neo4j.kernel.impl.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.register.Register;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.createIndex;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

public class IndexCRUDIT
{
    @Test
    public void addingANodeWithPropertyShouldGetIndexed() throws Exception
    {
        // Given
        String indexProperty = "indexProperty";
        GatheringIndexWriter writer = newWriter( indexProperty );
        createIndex( db, myLabel, indexProperty );

        // When
        int value1 = 12;
        String otherProperty = "otherProperty";
        int otherValue = 17;
        Node node = createNode( map( indexProperty, value1, otherProperty, otherValue ), myLabel );

        // Then, for now, this should trigger two NodePropertyUpdates
        try ( Transaction tx = db.beginTx() )
        {
            DataWriteOperations statement = ctxSupplier.get().dataWriteOperations();
            int propertyKey1 = statement.propertyKeyGetForName( indexProperty );
            long[] labels = new long[]{statement.labelGetForName( myLabel.name() )};
            assertThat( writer.updatesCommitted, equalTo( asSet(
                    NodePropertyUpdate.add( node.getId(), propertyKey1, value1, labels ) ) ) );
            tx.success();
        }
        // We get two updates because we both add a label and a property to be indexed
        // in the same transaction, in the future, we should optimize this down to
        // one NodePropertyUpdate.
    }

    @Test
    public void addingALabelToPreExistingNodeShouldGetIndexed() throws Exception
    {
        // GIVEN
        String indexProperty = "indexProperty";
        GatheringIndexWriter writer = newWriter( indexProperty );
        createIndex( db, myLabel, indexProperty );

        // WHEN
        String otherProperty = "otherProperty";
        int value = 12;
        int otherValue = 17;
        Node node = createNode( map( indexProperty, value, otherProperty, otherValue ) );

        // THEN
        assertThat( writer.updatesCommitted.size(), equalTo( 0 ) );

        // AND WHEN
        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( myLabel );
            tx.success();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            DataWriteOperations statement = ctxSupplier.get().dataWriteOperations();
            int propertyKey1 = statement.propertyKeyGetForName( indexProperty );
            long[] labels = new long[]{statement.labelGetForName( myLabel.name() )};
            assertThat( writer.updatesCommitted, equalTo( asSet(
                    NodePropertyUpdate.add( node.getId(), propertyKey1, value, labels ) ) ) );
            tx.success();
        }
    }

    @SuppressWarnings("deprecation") private GraphDatabaseAPI db;
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final SchemaIndexProvider mockedIndexProvider = mock( SchemaIndexProvider.class );
    private final KernelExtensionFactory<?> mockedIndexProviderFactory =
            singleInstanceSchemaIndexProviderFactory( "none", mockedIndexProvider );
    private ThreadToStatementContextBridge ctxSupplier;
    private final Label myLabel = label( "MYLABEL" );

    private Node createNode( Map<String, Object> properties, Label ... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String, Object> prop : properties.entrySet() )
            {
                node.setProperty( prop.getKey(), prop.getValue() );
            }
            tx.success();
            return node;
        }
    }

    @SuppressWarnings("deprecation")
    @Before
    public void before() throws Exception
    {
        when( mockedIndexProvider.storeMigrationParticipant( any( FileSystemAbstraction.class ), any( PageCache.class )
        ) ).thenReturn( StoreMigrationParticipant.NOT_PARTICIPATING );
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.addKernelExtensions(
                Collections.<KernelExtensionFactory<?>>singletonList( mockedIndexProviderFactory ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        ctxSupplier = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    private GatheringIndexWriter newWriter( String propertyKey ) throws IOException
    {
        GatheringIndexWriter writer = new GatheringIndexWriter( propertyKey );
        when( mockedIndexProvider.getPopulator(
                        anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                        any( IndexSamplingConfig.class ) )
        ).thenReturn( writer );
        when( mockedIndexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( mockedIndexProvider.getOnlineAccessor(
                anyLong(), any( IndexConfiguration.class ), any( IndexSamplingConfig.class )
        ) ).thenReturn( writer );
        when( mockedIndexProvider.compareTo( any( SchemaIndexProvider.class ) ) )
                .thenReturn( 1 ); // always pretend to have highest priority
        return writer;
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private class GatheringIndexWriter extends IndexAccessor.Adapter implements IndexPopulator
    {
        private final Set<NodePropertyUpdate> updatesCommitted = new HashSet<>();
        private final String propertyKey;
        private final Map<Object,Set<Long>> indexSamples = new HashMap<>();

        public GatheringIndexWriter( String propertyKey )
        {
            this.propertyKey = propertyKey;
        }

        @Override
        public void create()
        {
        }

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            ReadOperations statement = ctxSupplier.get().readOperations();
            updatesCommitted.add( NodePropertyUpdate.add(
                    nodeId, statement.propertyKeyGetForName( propertyKey ),
                    propertyValue, new long[]{statement.labelGetForName( myLabel.name() )} ) );
            addValueToSample( nodeId, propertyValue );
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
        {
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor ) throws IOException
        {
            return newUpdater( IndexUpdateMode.ONLINE );
        }

        @Override
        public IndexUpdater newUpdater( final IndexUpdateMode mode )
        {
            return new CollectingIndexUpdater()
            {
                @Override
                public void close() throws IOException, IndexEntryConflictException
                {
                    if ( IndexUpdateMode.ONLINE == mode )
                    {
                        updatesCommitted.addAll( updates );
                    }
                }

                @Override
                public void remove( PrimitiveLongSet nodeIds ) throws IOException
                {
                    throw new UnsupportedOperationException( "not expected" );
                }
            };
        }

        @Override
        public void close( boolean populationCompletedSuccessfully )
        {
        }

        @Override
        public void markAsFailed( String failure )
        {
        }

        @Override
        public long sampleResult( Register.DoubleLong.Out result )
        {
            long indexSize = 0;
            for ( Set<Long> nodeIds : indexSamples.values() )
            {
                indexSize += nodeIds.size();
            }

            result.write( indexSamples.size(), indexSize );
            return indexSize;
        }

        private void addValueToSample( long nodeId, Object propertyValue )
        {
            Set<Long> nodeIds = indexSamples.get( propertyValue );
            if ( nodeIds == null )
            {
                nodeIds = new HashSet<>();
                indexSamples.put( propertyValue, nodeIds );
            }
            nodeIds.add( nodeId );
        }
    }
}
