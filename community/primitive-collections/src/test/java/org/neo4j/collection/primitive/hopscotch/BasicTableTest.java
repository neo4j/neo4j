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
package org.neo4j.collection.primitive.hopscotch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.collection.primitive.Primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.collection.primitive.Primitive.VALUE_MARKER;

@RunWith( Parameterized.class )
public class BasicTableTest
{
    private final TableFactory factory;

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        Collection<Object[]> result = new ArrayList<>();
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new IntKeyTable( capacity, VALUE_MARKER );
            }

            @Override
            public boolean supportsLongs()
            {
                return false;
            }

            @Override
            public Object sampleValue()
            {
                return null;
            }
        } } );
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new LongKeyTable( capacity, VALUE_MARKER );
            }

            @Override
            public boolean supportsLongs()
            {
                return true;
            }

            @Override
            public Object sampleValue()
            {
                return null;
            }
        } } );
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new IntKeyUnsafeTable( capacity, VALUE_MARKER );
            }

            @Override
            public boolean supportsLongs()
            {
                return false;
            }

            @Override
            public Object sampleValue()
            {
                return null;
            }
        } } );
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new LongKeyUnsafeTable( capacity, VALUE_MARKER );
            }

            @Override
            public boolean supportsLongs()
            {
                return true;
            }

            @Override
            public Object sampleValue()
            {
                return null;
            }
        } } );
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new LongKeyIntValueTable( capacity );
            }

            @Override
            public boolean supportsLongs()
            {
                return true;
            }

            @Override
            public Object sampleValue()
            {
                return new int[] {4};
            }
        } } );
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new LongKeyObjectValueTable( capacity );
            }

            @Override
            public boolean supportsLongs()
            {
                return true;
            }

            @Override
            public Object sampleValue()
            {
                return new long[] {1458489572354L};
            }
        } } );
        result.add( new Object[] { new TableFactory()
        {
            @Override
            public Table newTable( int capacity )
            {
                return new LongKeyLongValueUnsafeTable( capacity );
            }

            @Override
            public boolean supportsLongs()
            {
                return true;
            }

            @Override
            public Object sampleValue()
            {
                return new long[] {1458489572354L};
            }
        } } );
        return result;
    }

    public BasicTableTest( TableFactory factory )
    {
        this.factory = factory;
    }

    @Test
    public void shouldSetAndGetSmallKey() throws Exception
    {
        // GIVEN
        try ( Table table = factory.newTable( Primitive.DEFAULT_HEAP_CAPACITY ) )
        {
            long nullKey = table.nullKey();
            assertEquals( nullKey, table.key( 0 ) );

            // WHEN
            long key = 12345;
            int index = 2;
            table.put( index, key, factory.sampleValue() );

            // THEN
            assertEquals( key, table.key( index ) );

            // WHEN/THEN
            table.remove( index );
            assertEquals( nullKey, table.key( index ) );
        }
    }

    @Test
    public void shouldSetAndGetBigKey() throws Exception
    {
        // GIVEN
        assumeTrue( factory.supportsLongs() );
        try ( Table table = factory.newTable( Primitive.DEFAULT_HEAP_CAPACITY ) )
        {
            long nullKey = table.nullKey();
            assertEquals( nullKey, table.key( 0 ) );

            // WHEN
            long key = 0x24FCFF2FFL;
            int index = 2;
            table.put( index, key, factory.sampleValue() );

            // THEN
            assertEquals( key, table.key( index ) );
        }
    }

    @Test
    public void shouldRemoveBigKey() throws Exception
    {
        // GIVEN
        assumeTrue( factory.supportsLongs() );
        try ( Table table = factory.newTable( Primitive.DEFAULT_HEAP_CAPACITY ) )
        {
            long nullKey = table.nullKey();
            long key = 0x24F1FF3FEL;
            int index = 5;
            table.put( index, key, factory.sampleValue() );
            assertEquals( key, table.key( index ) );

            // WHEN
            table.remove( index );

            // THEN
            assertEquals( nullKey, table.key( index ) );
        }
    }

    @Test
    public void shouldSetHopBits() throws Exception
    {
        // GIVEN
        try ( Table<?> table = factory.newTable( Primitive.DEFAULT_HEAP_CAPACITY ) )
        {
            int index = 10;
            long hopBits = table.hopBits( index );
            assertEquals( 0L, hopBits );

            // WHEN
            table.putHopBit( index, 2 );
            table.putHopBit( index, 11 );

            // THEN
            assertEquals( (1L << 2) | (1L << 11), table.hopBits( index ) );
        }
    }

    @Test
    public void shouldMoveHopBit() throws Exception
    {
        // GIVEN
        try ( Table<?> table = factory.newTable( Primitive.DEFAULT_HEAP_CAPACITY ) )
        {
            int index = 10;
            table.putHopBit( index, 2 );
            table.putHopBit( index, 11 );

            // WHEN
            table.moveHopBit( index, 2, 15 ); // will end up at 17

            // THEN
            assertEquals( (1L << 11) | (1L << 17), table.hopBits( index ) );
        }
    }

    private interface TableFactory
    {
        Table newTable( int capacity );

        Object sampleValue();

        boolean supportsLongs();
    }
}
