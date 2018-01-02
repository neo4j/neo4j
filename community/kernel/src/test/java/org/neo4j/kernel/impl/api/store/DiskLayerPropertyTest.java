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
package org.neo4j.kernel.impl.api.store;

import java.lang.reflect.Array;

import org.junit.Test;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

import static java.util.Collections.singletonMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test read access to committed properties.
 */
public class DiskLayerPropertyTest extends DiskLayerTest
{
    @Test
    public void should_get_all_node_properties() throws Exception
    {
        // GIVEN
        String longString =
                "AlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalongAlalalalalong";
        Object[] properties = {
                longString,
                createNew( String.class ),
                createNew( long.class ),
                createNew( int.class ),
                createNew( byte.class ),
                createNew( short.class ),
                createNew( boolean.class ),
                createNew( char.class ),
                createNew( float.class ),
                createNew( double.class ),
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

        int propKey = disk.propertyKeyGetOrCreateForName( "prop" );

        StoreStatement statement = state.getStoreStatement();
        for ( Object value : properties )
        {
            // given
            long nodeId = createLabeledNode( db, singletonMap( "prop", value ), label1 ).getId();

            // when
            try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( nodeId ) )
            {
                node.next();

                try ( Cursor<PropertyItem> props = node.get().property( propKey ) )
                {
                    if ( props.next() )
                    {
                        Object propVal = props.get().value();

                        //then
                        assertTrue( propVal + ".valueEquals(" + value + ")",
                                Property.property( propKey, propVal ).valueEquals( value ) );
                    }
                    else
                    {
                        fail();
                    }
                }
            }

        }
    }

    @Test
    public void should_create_property_key_if_not_exists() throws Exception
    {
        // WHEN
        long id = disk.propertyKeyGetOrCreateForName( propertyKey );

        // THEN
        assertTrue( "Should have created a non-negative id", id >= 0 );
    }

    @Test
    public void should_get_previously_created_property_key() throws Exception
    {
        // GIVEN
        long id = disk.propertyKeyGetOrCreateForName( propertyKey );

        // WHEN
        long secondId = disk.propertyKeyGetForName( propertyKey );

        // THEN
        assertEquals( id, secondId );
    }

    @Test
    public void should_be_able_to_get_or_create_previously_created_property_key() throws Exception
    {
        // GIVEN
        long id = disk.propertyKeyGetOrCreateForName( propertyKey );

        // WHEN
        long secondId = disk.propertyKeyGetOrCreateForName( propertyKey );

        // THEN
        assertEquals( id, secondId );
    }

    @Test
    public void should_fail_if_get_non_existent_property_key() throws Exception
    {
        // WHEN
        int propertyKey = disk.propertyKeyGetForName( "non-existent-property-key" );

        // THEN
        assertEquals( KeyReadOperations.NO_SUCH_PROPERTY_KEY, propertyKey );
    }

    private Object array( int length, Class<?> componentType )
    {
        Object array = Array.newInstance( componentType, length );
        for ( int i = 0; i < length; i++ )
        {
            Array.set( array, i, createNew( componentType ) );
        }
        return array;
    }

    private Object createNew( Class<?> type )
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
