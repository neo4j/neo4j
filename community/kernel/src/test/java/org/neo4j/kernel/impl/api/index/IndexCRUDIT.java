/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.extension.KernelExtensionFactory;
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
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

public class IndexCRUDIT
{

    private Transaction transaction;

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
        Transaction tx = db.beginTx();
        try
        {
            StatementOperations context = ctxProvider.getCtxForWriting().asStatementOperations();
            StatementState state = ctxProvider.statementForWriting();
            long propertyKey1 = context.propertyKeyGetForName( state, indexProperty );
            long[] labels = new long[]{context.labelGetForName( state, myLabel.name() )};
            assertThat( writer.updates, equalTo( asSet(
                    NodePropertyUpdate.add( node.getId(), propertyKey1, value1, labels ) ) ) );
        }
        finally
        {
            tx.finish();
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
        assertThat( writer.updates.size(), equalTo( 0 ) );

        // AND WHEN
        Transaction tx = db.beginTx();
        node.addLabel( myLabel );
        tx.success();
        tx.finish();

        // THEN
        tx = db.beginTx();
        try
        {
            StatementOperations context = ctxProvider.getCtxForWriting().asStatementOperations();
            StatementState state = ctxProvider.statementForWriting();
            long propertyKey1 = context.propertyKeyGetForName( state, indexProperty );
            long[] labels = new long[]{context.labelGetForName( state, myLabel.name() )};
            assertThat( writer.updates, equalTo( asSet(
                    NodePropertyUpdate.add( node.getId(), propertyKey1, value, labels ) ) ) );
        }
        finally
        {
            tx.finish();
        }

    }

    private GraphDatabaseAPI db;
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final SchemaIndexProvider mockedIndexProvider = mock( SchemaIndexProvider.class );
    private final KernelExtensionFactory<?> mockedIndexProviderFactory =
            singleInstanceSchemaIndexProviderFactory( "none", mockedIndexProvider );
    private ThreadToStatementContextBridge ctxProvider;
    private final Label myLabel = label( "MYLABEL" );
    
    private Node createNode( Map<String, Object> properties, Label ... labels )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode( labels );
        for ( Map.Entry<String, Object> prop : properties.entrySet() )
        {
            node.setProperty( prop.getKey(), prop.getValue() );
        }
        tx.success();
        tx.finish();
        return node;
    }

    @Before
    public void before() throws Exception
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.setKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( mockedIndexProviderFactory ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        ctxProvider = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }
    
    private GatheringIndexWriter newWriter( String propertyKey ) throws IOException
    {
        GatheringIndexWriter writer = new GatheringIndexWriter( propertyKey );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( writer );
        when( mockedIndexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( mockedIndexProvider.getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( writer );
        return writer;
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
    
    private class GatheringIndexWriter extends IndexAccessor.Adapter implements IndexPopulator
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<NodePropertyUpdate>();
        private final String propertyKey;
        
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
            try
            {
                StatementOperations context = ctxProvider.getCtxForReading().asStatementOperations();
                StatementState state = ctxProvider.statementForReading();
                updates.add( NodePropertyUpdate.add( nodeId, context.propertyKeyGetForName( state, propertyKey ),
                        propertyValue, new long[] {context.labelGetForName( state, myLabel.name() )} ) );
            }
            catch ( PropertyKeyNotFoundException e )
            {
                throw new RuntimeException( e );
            }
            catch ( LabelNotFoundKernelException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( updates ) );
        }
        
        @Override
        public void updateAndCommit( Iterable<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( updates ) );
        }

        @Override
        public void close( boolean populationCompletedSuccessfully )
        {
        }
        
        @Override
        public void markAsFailed( String failure )
        {
        }
    }
}
