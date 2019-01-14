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
package org.neo4j.kernel.impl.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;


public class PropertyPhysicalToLogicalConverterTest
{

    private NeoStores neoStores;

    @Test
    public void shouldConvertInlinedAddedProperty()
    {
        // GIVEN
        int key = 10;
        Value value = Values.of( 12345 );
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        assertThat(
                convert( none, none, change( before, after ) ),
                equalTo( NodeUpdates.forNode( 0 ).added( key, value ).build() ) );
    }

    @Test
    public void shouldConvertInlinedChangedProperty()
    {
        // GIVEN
        int key = 10;
        Value valueBefore = Values.of( 12341 );
        Value valueAfter = Values.of( 738 );
        PropertyRecord before = propertyRecord( property( key, valueBefore ) );
        PropertyRecord after = propertyRecord( property( key, valueAfter ) );

        // WHEN
        NodeUpdates update = convert( none, none, change( before, after ) );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( 0 ).changed( key, valueBefore, valueAfter ).build();
        assertEquals( expected, update );
    }

    @Test
    public void shouldIgnoreInlinedUnchangedProperty()
    {
        // GIVEN
        int key = 10;
        Value value = Values.of( 12341 );
        PropertyRecord before = propertyRecord( property( key, value ) );
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        assertThat(
                convert( none, none, change( before, after ) ),
                equalTo( NodeUpdates.forNode( 0 ).build() ) );
    }

    @Test
    public void shouldConvertInlinedRemovedProperty()
    {
        // GIVEN
        int key = 10;
        Value value = Values.of( 12341 );
        PropertyRecord before = propertyRecord( property( key, value ) );
        PropertyRecord after = propertyRecord();

        // WHEN
        NodeUpdates update = convert( none, none, change( before, after ) );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( 0 ).removed( key, value ).build();
        assertEquals( expected, update );
    }

    @Test
    public void shouldConvertDynamicAddedProperty()
    {
        // GIVEN
        int key = 10;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, longString ) );

        // THEN
        assertThat(
                convert( none, none, change( before, after ) ),
                equalTo( NodeUpdates.forNode( 0 ).added( key, longString ).build() ) );
    }

    @Test
    public void shouldConvertDynamicChangedProperty()
    {
        // GIVEN
        int key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord( property( key, longerString ) );

        // WHEN
        NodeUpdates update = convert( none, none, change( before, after ) );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( 0 ).changed( key, longString, longerString ).build();
        assertEquals( expected, update );
    }

    @Test
    public void shouldConvertDynamicInlinedRemovedProperty()
    {
        // GIVEN
        int key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord();

        // WHEN
        NodeUpdates update = convert( none, none, change( before, after ) );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( 0 ).removed( key, longString ).build();
        assertEquals( expected, update );
    }

    @Test
    public void shouldTreatPropertyThatMovedToAnotherRecordAsChange()
    {
        // GIVEN
        int key = 12;
        Value oldValue = Values.of( "value1" );
        Value newValue = Values.of( "value two" );
        PropertyRecordChange movedFrom = change(
                propertyRecord( property( key, oldValue ) ),
                propertyRecord() );
        PropertyRecordChange movedTo = change(
                propertyRecord(),
                propertyRecord( property( key, newValue ) ) );

        // WHEN
        NodeUpdates update = convert( none, none, movedFrom, movedTo );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( 0 ).changed( key, oldValue, newValue ).build();
        assertEquals( expected, update );
    }

    private PropertyRecord propertyRecord( PropertyBlock... propertyBlocks )
    {
        PropertyRecord record = new PropertyRecord( 0 );
        if ( propertyBlocks != null )
        {
            record.setInUse( true );
            for ( PropertyBlock propertyBlock : propertyBlocks )
            {
                record.addPropertyBlock( propertyBlock );
            }
        }
        record.setNodeId( 0 );
        return record;
    }

    private PropertyBlock property( long key, Value value )
    {
        PropertyBlock block = new PropertyBlock();
        store.encodeValue( block, (int) key, value );
        return block;
    }

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private PropertyStore store;
    private final Value longString = Values.of(
            "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiing" );
    private final Value longerString = Values.of(
            "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiingdd" );
    private PropertyPhysicalToLogicalConverter converter;
    private final long[] none = new long[0];
    private final long[] labels = new long[]{11};

    @Before
    public void before()
    {
        File storeDir = new File( "dir" );
        fs.get().mkdirs( storeDir );
        StoreFactory storeFactory = new StoreFactory( storeDir, Config.defaults(), new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance(),
                EmptyVersionContextSupplier.EMPTY );
        neoStores = storeFactory.openAllNeoStores( true );
        store = neoStores.getPropertyStore();
        converter = new PropertyPhysicalToLogicalConverter( store );
    }

    @After
    public void after()
    {
        neoStores.close();
    }

    private NodeUpdates convert( long[] labelsBefore,
            long[] labelsAfter, PropertyRecordChange change )
    {
        return convert( labelsBefore, labelsAfter, new PropertyRecordChange[] {change} );
    }

    private NodeUpdates convert( long[] labelsBefore,
            long[] labelsAfter, PropertyRecordChange... changes )
    {
        NodeUpdates.Builder updates = NodeUpdates.forNode( 0, labelsBefore, labelsAfter );
        converter.convertPropertyRecord( 0, Iterables.iterable( changes ), updates );
        return updates.build();
    }

    private PropertyRecordChange change( final PropertyRecord before, final PropertyRecord after )
    {
        return new PropertyRecordChange()
        {
            @Override
            public PropertyRecord getBefore()
            {
                return before;
            }

            @Override
            public PropertyRecord getAfter()
            {
                return after;
            }
        };
    }
}
