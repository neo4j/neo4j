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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.impl.core.IteratingPropertyReceiver;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableRuleMigrationSupport
@ExtendWith( EmbeddedDatabaseExtension.class )
public class PropertyLoaderTest
{
    private static final int PROP_KEY_ID = 42;

    @Resource
    public EmbeddedDatabaseRule db;

    private final IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();

    private final NeoStores neoStores = mock( NeoStores.class );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final RelationshipStore relationshipStore = mock( RelationshipStore.class );
    private final PropertyStore propertyStore = mock( PropertyStore.class );

    @BeforeEach
    public void setUpMocking()
    {
        doReturn( nodeStore ).when( neoStores ).getNodeStore();
        when( nodeStore.newRecord() ).thenAnswer( invocation -> new NodeRecord( -1 ) );
        doReturn( relationshipStore ).when( neoStores ).getRelationshipStore();
        when( relationshipStore.newRecord() ).thenAnswer( invocation -> new RelationshipRecord( -1 ) );
        doReturn( propertyStore ).when( neoStores ).getPropertyStore();
        when( propertyStore.newRecord() ).thenAnswer( invocation -> new PropertyRecord( -1 ) );
    }

    @Test
    public void shouldThrowForNotInUseNodeRecord()
    {
        // Given
        PropertyLoader loader = new PropertyLoader( neoStores() );

        try
        {
            // When
            loader.nodeLoadProperties( 42, receiver );
        }
        catch ( InvalidRecordException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Node" ) );
        }
    }

    @Test
    public void shouldThrowForNotInUseRelationshipRecord()
    {
        // Given
        PropertyLoader loader = new PropertyLoader( neoStores() );

        try
        {
            // When
            loader.relLoadProperties( 42, receiver );
        }
        catch ( InvalidRecordException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Relationship" ) );
        }
    }

    @Test
    public void shouldReturnCorrectPropertyChainForNode()
    {
        // Given
        setUpNode( 42, 1, 2, 3 );
        PropertyLoader loader = new PropertyLoader( neoStores );

        // When
        loader.nodeLoadProperties( 42, receiver );

        // Then
        assertEquals(
                asList( intProperty( PROP_KEY_ID, 1 ), intProperty( PROP_KEY_ID, 2 ), intProperty( PROP_KEY_ID, 3 ) ),
                Iterators.asList( receiver ) );
    }

    @Test
    public void shouldReturnCorrectPropertyChainForRelationship()
    {
        // Given
        setUpRelationship( 42, 1111, 2222 );
        PropertyLoader loader = new PropertyLoader( neoStores );

        // When
        loader.relLoadProperties( 42, receiver );

        // Then
        assertEquals(
                asList( intProperty( PROP_KEY_ID, 1111 ), intProperty( PROP_KEY_ID, 2222 ) ),
                Iterators.asList( receiver ) );
    }

    private NeoStores neoStores()
    {
        return db.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
    }

    private void setUpNode( long id, int... propertyValues )
    {
        setUpPropertyChain( id, NodeRecord.class, nodeStore, propertyValues );
    }

    private void setUpRelationship( long id, int... propertyValues )
    {
        setUpPropertyChain( id, RelationshipRecord.class, relationshipStore, propertyValues );
    }

    private <R extends PrimitiveRecord> void setUpPropertyChain( long id, Class<R> recordClass,
            CommonAbstractStore<R,? extends StoreHeader> store, int... propertyValues )
    {
        when( store.getRecord( eq( id ), any( recordClass ), any( RecordLoad.class ) ) ).thenAnswer( invocation ->
        {
            R record = invocation.getArgument( 1 );
            record.setId( ((Number) invocation.getArgument( 0 )).longValue() );
            record.setNextProp( 1 );
            return record;
        } );
        List<PropertyRecord> propertyChain = new ArrayList<>( propertyValues.length );
        for ( int i = 0; i < propertyValues.length; i++ )
        {
            propertyChain.add( newSingleIntProperty( i + 1, propertyValues[i] ) );
        }
        doReturn( propertyChain ).when( propertyStore ).getPropertyRecordChain( 1L );
    }

    private static PropertyRecord newSingleIntProperty( long id, int value )
    {
        PropertyRecord record = new PropertyRecord( id );
        record.setInUse( true );
        record.addPropertyBlock( newSingleIntPropertyBlock( value ) );
        return record;
    }

    private static PropertyBlock newSingleIntPropertyBlock( int value )
    {
        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue( block, PROP_KEY_ID, Values.intValue( value ), null, null, true );
        block.setKeyIndexId( PROP_KEY_ID );
        return block;
    }

    private StorageProperty intProperty( int propKeyId, int value )
    {
        return new PropertyKeyValue( propKeyId, Values.of( value ) );
    }
}
