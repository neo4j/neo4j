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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.single;


public class PropertyPhysicalToLogicalConverterTest
{

    private NeoStores neoStores;

    @Test
    public void shouldNotConvertInlinedAddedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12345;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        List<NodePropertyUpdate> updates = toList( convert( none, none, change( before, after ) ) );

        // THEN
        assertEquals( Collections.<NodePropertyUpdate>emptyList(), updates );
    }

    @Test
    public void shouldConvertInlinedChangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int valueBefore = 12341, valueAfter = 738;
        PropertyRecord before = propertyRecord( property( key, valueBefore ) );
        PropertyRecord after = propertyRecord( property( key, valueAfter ) );

        // WHEN
        NodePropertyUpdate update = single( convert( none, none, change( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.CHANGED, update.getUpdateMode() );
    }

    @Test
    public void shouldIgnoreInlinedUnchangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12341;
        PropertyRecord before = propertyRecord( property( key, value ) );
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        assertEquals( 0, count( convert( none, none, change( before, after ) ) ) );
    }

    @Test
    public void shouldConvertInlinedRemovedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12341;
        PropertyRecord before = propertyRecord( property( key, value ) );
        PropertyRecord after = propertyRecord();

        // WHEN
        NodePropertyUpdate update = single( convert( none, none, change( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.REMOVED, update.getUpdateMode() );
    }

    @Test
    public void shouldNotConvertDynamicAddedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, longString ) );

        // WHEN
        List<NodePropertyUpdate> updates = toList( convert( none, none, change( before, after ) ) );

        // THEN
        assertEquals( Collections.<NodePropertyUpdate>emptyList(), updates );
    }

    @Test
    public void shouldConvertDynamicChangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord( property( key, longerString ) );

        // WHEN
        NodePropertyUpdate update = single( convert( none, none, change( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.CHANGED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertDynamicInlinedRemovedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord();

        // WHEN
        NodePropertyUpdate update = single( convert( none, none, change( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.REMOVED, update.getUpdateMode() );
    }

    @Test
    public void shouldTreatPropertyThatMovedToAnotherRecordAsChange() throws Exception
    {
        // GIVEN
        long key = 12;
        String oldValue = "value1";
        String newValue = "value two";
        PropertyRecordChange movedFrom = change(
                propertyRecord( property( key, oldValue ) ),
                propertyRecord() );
        PropertyRecordChange movedTo = change(
                propertyRecord(),
                propertyRecord( property( key, newValue ) ) );

        // WHEN
        NodePropertyUpdate update = single( convert( none, none, movedFrom, movedTo ) );

        // THEN
        assertEquals( UpdateMode.CHANGED, update.getUpdateMode() );
        assertEquals( oldValue, update.getValueBefore() );
        assertEquals( newValue, update.getValueAfter() );
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
        return record;
    }

    private PropertyBlock property( long key, Object value )
    {
        PropertyBlock block = new PropertyBlock();
        store.encodeValue( block, (int) key, value );
        return block;
    }

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private PropertyStore store;
    private final String longString = "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiing";
    private final String longerString = "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiingdd";
    private PropertyPhysicalToLogicalConverter converter;
    private final long[] none = new long[0];

    @Before
    public void before() throws Exception
    {
        File storeDir = new File( "dir" );
        fs.get().mkdirs( storeDir );
        StoreFactory storeFactory = new StoreFactory( storeDir, new Config(), new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
        store = neoStores.getPropertyStore();
        converter = new PropertyPhysicalToLogicalConverter( store );
    }

    @After
    public void after() throws Exception
    {
        neoStores.close();
    }

    private Iterable<NodePropertyUpdate> convert( long[] labelsBefore,
            long[] labelsAfter, PropertyRecordChange change )
    {
        return convert( labelsBefore, labelsAfter, new PropertyRecordChange[] {change} );
    }

    private Iterable<NodePropertyUpdate> convert( long[] labelsBefore,
            long[] labelsAfter, PropertyRecordChange... changes )
    {
        Collection<NodePropertyUpdate> updates = new ArrayList<>();
        converter.apply( updates, Iterables.<PropertyRecordChange,PropertyRecordChange>iterable( changes ),
                labelsBefore, labelsAfter );
        return updates;
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
