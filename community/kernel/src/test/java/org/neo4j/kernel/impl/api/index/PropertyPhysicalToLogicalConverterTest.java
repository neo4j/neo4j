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

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.UpdateMode;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class PropertyPhysicalToLogicalConverterTest
{
    @Test
    public void shouldConvertInlinedAddedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12345;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        NodePropertyUpdate update = single( converter.apply( before, none, after, none ) );

        // THEN
        assertEquals( UpdateMode.ADDED, update.getUpdateMode() );
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
        NodePropertyUpdate update = single( converter.apply( before, none, after, none ) );

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
        assertEquals( 0, count( converter.apply( before, none, after, none ) ) );
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
        NodePropertyUpdate update = single( converter.apply( before, none, after, none ) );

        // THEN
        assertEquals( UpdateMode.REMOVED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertDynamicAddedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, longString ) );

        // WHEN
        NodePropertyUpdate update = single( converter.apply( before, none, after, none ) );

        // THEN
        assertEquals( UpdateMode.ADDED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertDynamicChangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord( property( key, longerString ) );

        // WHEN
        NodePropertyUpdate update = single( converter.apply( before, none, after, none ) );

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
        NodePropertyUpdate update = single( converter.apply( before, none, after, none ) );

        // THEN
        assertEquals( UpdateMode.REMOVED, update.getUpdateMode() );
    }
    
    private PropertyRecord propertyRecord( PropertyBlock... propertyBlocks )
    {
        PropertyRecord record = new PropertyRecord( 0 );
        if ( propertyBlocks != null )
        {
            record.setInUse( true );
            for ( PropertyBlock propertyBlock : propertyBlocks )
                record.addPropertyBlock( propertyBlock );
        }
        return record;
    }

    private PropertyBlock property( long key, Object value )
    {
        PropertyBlock block = new PropertyBlock();
        store.encodeValue( block, (int) key, value );
        return block;
    }
    
    private EphemeralFileSystemAbstraction fs;
    private PropertyStore store;
    private final String longString = "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiing";
    private final String longerString = "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiingdd";
    private PropertyPhysicalToLogicalConverter converter;
    private final long[] none = new long[0];
    
    @Before
    public void before() throws Exception
    {
        fs = new EphemeralFileSystemAbstraction();
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
        File storeFile = new File( "propertystore" );
        storeFactory.createPropertyStore( storeFile );
        store = storeFactory.newPropertyStore( storeFile );
        converter = new PropertyPhysicalToLogicalConverter( store );
    }

    @After
    public void after() throws Exception
    {
        store.close();
        fs.shutdown();
    }
}
