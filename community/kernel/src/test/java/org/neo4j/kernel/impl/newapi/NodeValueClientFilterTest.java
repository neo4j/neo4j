/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.values.storable.Values.stringValue;

public class NodeValueClientFilterTest implements IndexProgressor, NodeValueClient
{
    private final Read read = mock( Read.class );
    private final List<Event> events = new ArrayList<>();
    private final StubNodeCursor node = new StubNodeCursor();
    private final StubPropertyCursor property = new StubPropertyCursor();

    @Test
    public void shouldAcceptAllNodesOnNoFilters()
    {
        // given
        node.withNode( 17 );
        NodeValueClientFilter filter = initializeFilter();

        // when
        filter.next();
        assertTrue( filter.acceptNode( 17, null ) );
        filter.close();

        // then
        assertEvents( initialize(), Event.NEXT, new Event.Node( 17, null ), Event.CLOSE );
    }

    @Test
    public void shouldRejectNodeNotInUse()
    {
        // given
        NodeValueClientFilter filter = initializeFilter( IndexQuery.exists( 12 ) );

        // when
        filter.next();
        assertFalse( filter.acceptNode( 17, null ) );
        filter.close();

        // then
        assertEvents( initialize(), Event.NEXT, Event.CLOSE );
    }

    @Test
    public void shouldRejectNodeWithNoProperties()
    {
        // given
        node.withNode( 17 );
        NodeValueClientFilter filter = initializeFilter( IndexQuery.exists( 12 ) );

        // when
        filter.next();
        assertFalse( filter.acceptNode( 17, null ) );
        filter.close();

        // then
        assertEvents( initialize(), Event.NEXT, Event.CLOSE );
    }

    @Test
    public void shouldAcceptNodeWithMatchingProperty()
    {
        // given
        node.withNode( 17, new long[0], genericMap( 12, stringValue( "hello" ) ) );
        NodeValueClientFilter filter = initializeFilter( IndexQuery.exists( 12 ) );

        // when
        filter.next();
        assertTrue( filter.acceptNode( 17, null ) );
        filter.close();

        // then
        assertEvents( initialize(), Event.NEXT, new Event.Node( 17, null ), Event.CLOSE );
    }

    @Test
    public void shouldNotAcceptNodeWithoutMatchingProperty()
    {
        // given
        node.withNode( 17, new long[0], genericMap( 7, stringValue( "wrong" ) ) );
        NodeValueClientFilter filter = initializeFilter( IndexQuery.exists( 12 ) );

        // when
        filter.next();
        assertFalse( filter.acceptNode( 17, null ) );
        filter.close();

        // then
        assertEvents( initialize(), Event.NEXT, Event.CLOSE );
    }

    private NodeValueClientFilter initializeFilter( IndexQuery... filters )
    {
        NodeValueClientFilter filter = new NodeValueClientFilter(
                this, node, property, read, filters );
        filter.initialize( TestIndexDescriptorFactory.forLabel( 11), this, null, true );
        return filter;
    }

    private void assertEvents( Event... expected )
    {
        assertEquals( Arrays.asList( expected ), events );
    }

    private Event.Initialize initialize( int... keys )
    {
        return new Event.Initialize( this, keys );
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] queries, boolean needsValues )
    {
        events.add( new Event.Initialize( progressor, descriptor.schema().getPropertyIds() ) );
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        events.add( new Event.Node( reference, values ) );
        return true;
    }

    @Override
    public boolean needsValues()
    {
        return true;
    }

    @Override
    public boolean next()
    {
        events.add( Event.NEXT );
        return true;
    }

    @Override
    public void close()
    {
        events.add( Event.CLOSE );
    }

    private abstract static class Event
    {
        static class Initialize extends Event
        {
            final transient IndexProgressor progressor;
            final int[] keys;

            Initialize( IndexProgressor progressor, int[] keys )
            {
                this.progressor = progressor;
                this.keys = keys;
            }

            @Override
            public String toString()
            {
                return "INITIALIZE(" + Arrays.toString( keys ) + ")";
            }
        }

        static final Event CLOSE = new Event()
        {
            @Override
            public String toString()
            {
                return "CLOSE";
            }
        };

        static final Event NEXT = new Event()
        {
            @Override
            public String toString()
            {
                return "NEXT";
            }
        };

        static class Node extends Event
        {
            final long reference;
            final Value[] values;

            Node( long reference, Value[] values )
            {
                this.reference = reference;
                this.values = values;
            }

            @Override
            public String toString()
            {
                return "Node(" + reference + "," + Arrays.toString( values ) + ")";
            }
        }

        @Override
        public final boolean equals( Object other )
        {
            return toString().equals( other.toString() );
        }

        @Override
        public final int hashCode()
        {
            return toString().hashCode();
        }
    }
}
