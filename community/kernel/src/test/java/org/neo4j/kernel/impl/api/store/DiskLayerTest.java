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
package org.neo4j.kernel.impl.api.store;

import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.getPropertyKeys;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cache_type;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

public class DiskLayerTest
{
    @Test
    public void should_be_able_to_list_labels_for_node() throws Exception
    {
        // GIVEN
        long nodeId;
        int labelId1, labelId2;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode( label1, label2 ).getId();
            String labelName1 = label1.name(), labelName2 = label2.name();
            labelId1 = statement.labelGetForName( labelName1 );
            labelId2 = statement.labelGetOrCreateForName( labelName2 );
            tx.success();
        }

        // THEN
        PrimitiveIntIterator readLabels = statement.nodeGetLabels( nodeId );
        assertEquals( new HashSet<>( asList( labelId1, labelId2 ) ),
                addToCollection( readLabels, new HashSet<Integer>() ) );
    }

    @Test
    public void should_be_able_to_get_label_name_for_label() throws Exception
    {
        // GIVEN
        String labelName = label1.name();
        int labelId = statement.labelGetOrCreateForName( labelName );

        // WHEN
        String readLabelName = statement.labelGetName( labelId );

        // THEN
        assertEquals( labelName, readLabelName );
    }

    /*
     * This test doesn't really belong here, but OTOH it does, as it has to do with this specific
     * store solution. It creates its own IGD with cache_type:none to try reproduce to trigger the problem.
     */
    @Test
    public void labels_should_not_leak_out_as_properties() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( cache_type, "none" ).newGraphDatabase();
        Node node = createLabeledNode( db, map( "name", "Node" ), label1 );

        // WHEN THEN
        assertThat( getPropertyKeys( db, node ), containsOnly( "name" ) );
    }

    @Test
    public void should_return_all_nodes_with_label() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( db, map( "name", "First", "age", 1L ), label1 );
        Node node2 = createLabeledNode( db, map( "type", "Node", "count", 10 ), label1, label2 );
        int labelId1 = statement.labelGetForName( label1.name() );
        int labelId2 = statement.labelGetForName( label2.name() );

        // WHEN
        PrimitiveLongIterator nodesForLabel1 = statement.nodesGetForLabel( state, labelId1 );
        PrimitiveLongIterator nodesForLabel2 = statement.nodesGetForLabel( state, labelId2 );

        // THEN
        assertEquals( asSet( node1.getId(), node2.getId() ), IteratorUtil.asSet( nodesForLabel1 ) );
        assertEquals( asSet( node2.getId() ), IteratorUtil.asSet( nodesForLabel2 ) );
    }

    @Test
    public void should_get_all_node_properties() throws Exception
    {
        // GIVEN
        String longString =
                "AlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalong";
        Object[] properties = {
                longString,
                gimme( String.class ),
                gimme( long.class ),
                gimme( int.class ),
                gimme( byte.class ),
                gimme( short.class ),
                gimme( boolean.class ),
                gimme( char.class ),
                gimme( float.class ),
                gimme( double.class ),
                array( 0, String.class ),
                array( 0, long.class ),
                array( 0, int.class ),
                array( 0, byte.class ),
                array( 0, short.class ),
                array( 0, boolean.class ),
                array( 0, char.class ),
                array( 0, float.class ),
                array( 0, double.class ),
                array( 1, String.class ),
                array( 1, long.class ),
                array( 1, int.class ),
                array( 1, byte.class ),
                array( 1, short.class ),
                array( 1, boolean.class ),
                array( 1, char.class ),
                array( 1, float.class ),
                array( 1, double.class ),
                array( 256, String.class ),
                array( 256, long.class ),
                array( 256, int.class ),
                array( 256, byte.class ),
                array( 256, short.class ),
                array( 256, boolean.class ),
                array( 256, char.class ),
                array( 256, float.class ),
                array( 256, double.class ),
        };

        for ( Object value : properties )
        {
            // given
            long nodeId = createLabeledNode( db, singletonMap( "prop", value ), label1 ).getId();

            // when
            Property property = single( statement.nodeGetAllProperties( nodeId ) );

            //then
            assertTrue( property + ".valueEquals(" + value + ")", property.valueEquals( value ) );
        }
    }

    @Test
    public void should_create_property_key_if_not_exists() throws Exception
    {
        // WHEN
        long id = statement.propertyKeyGetOrCreateForName( propertyKey );

        // THEN
        assertTrue( "Should have created a non-negative id", id >= 0 );
    }

    @Test
    public void should_get_previously_created_property_key() throws Exception
    {
        // GIVEN
        long id = statement.propertyKeyGetOrCreateForName( propertyKey );

        // WHEN
        long secondId = statement.propertyKeyGetForName( propertyKey );

        // THEN
        assertEquals( id, secondId );
    }

    @Test
    public void should_be_able_to_get_or_create_previously_created_property_key() throws Exception
    {
        // GIVEN
        long id = statement.propertyKeyGetOrCreateForName( propertyKey );

        // WHEN
        long secondId = statement.propertyKeyGetOrCreateForName( propertyKey );

        // THEN
        assertEquals( id, secondId );
    }

    @Test
    public void should_fail_if_get_non_existent_property_key() throws Exception
    {
        // WHEN
        int propertyKey = statement.propertyKeyGetForName( "non-existent-property-key" );

        // THEN
        assertEquals( KeyReadOperations.NO_SUCH_PROPERTY_KEY, propertyKey );
    }

    @Test
    public void should_find_nodes_with_given_label_and_property_via_index() throws Exception
    {
        // GIVEN
        IndexDescriptor index = createIndexAndAwaitOnline( label1, propertyKey );
        String name = "Mr. Taylor";
        Node mrTaylor = createLabeledNode( db, map( propertyKey, name ), label1 );
        try ( Transaction ignored = db.beginTx() )
        {
            // WHEN
            Set<Long> foundNodes = asUniqueSet( statement.nodesGetFromIndexLookup( state, 1l, name ) );

            // THEN
            assertEquals( asSet( mrTaylor.getId() ), foundNodes );
        }
    }

    @SuppressWarnings("deprecation") private GraphDatabaseAPI db;
    private final Label label1 = label( "first-label" ), label2 = label( "second-label" );
    private final String propertyKey = "name";
    private KernelStatement state;
    private DiskLayer statement;

    @SuppressWarnings("deprecation")
    @Before
    public void before()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        DependencyResolver resolver = db.getDependencyResolver();
        IndexingService indexingService = resolver.resolveDependency( IndexingService.class );
        NeoStore neoStore = resolver.resolveDependency( XaDataSourceManager.class )
                .getNeoStoreDataSource().getNeoStore();
        this.statement = new DiskLayer(
                resolver.resolveDependency( PropertyKeyTokenHolder.class ),
                resolver.resolveDependency( LabelTokenHolder.class ),
                resolver.resolveDependency( RelationshipTypeTokenHolder.class ),
                new SchemaStorage( neoStore.getSchemaStore() ),
                singletonProvider( neoStore ),
                indexingService );
        this.state = new KernelStatement( null, new IndexReaderFactory.Caching( indexingService ),
                resolver.resolveDependency( LabelScanStore.class ), null,
                null, null, null, null );
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    private static Node createLabeledNode( GraphDatabaseService db, Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String, Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.success();
            return node;
        }
    }

    private IndexDescriptor createIndexAndAwaitOnline( Label label, String propertyKey ) throws Exception
    {
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }

        try ( Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 10, SECONDS );
            return statement.indexesGetForLabelAndPropertyKey( statement.labelGetForName( label.name() ),
                    statement.propertyKeyGetForName( propertyKey ) );
        }
    }

    private Object array( int length, Class<?> componentType )
    {
        Object array = Array.newInstance( componentType, length );
        for ( int i = 0; i < length; i++ )
        {
            Array.set( array, i, gimme( componentType ) );
        }
        return array;
    }

    private Object gimme( Class<?> type )
    {
        if ( type == int.class )
        {
            return 666;
        }
        if ( type == long.class )
        {
            return 17l;
        }
        if ( type == double.class )
        {
            return 6.28318530717958647692d;
        }
        if ( type == float.class )
        {
            return 3.14f;
        }
        if ( type == short.class )
        {
            return (short) 8733;
        }
        if ( type == byte.class )
        {
            return (byte) 123;
        }
        if ( type == boolean.class )
        {
            return false;
        }
        if ( type == char.class )
        {
            return 'Z';
        }
        if ( type == String.class )
        {
            return "hello world";
        }
        throw new IllegalArgumentException( type.getName() );
    }
}
