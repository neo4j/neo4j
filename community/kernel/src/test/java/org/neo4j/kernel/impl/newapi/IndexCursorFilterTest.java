/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.newapi.MockStore.block;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.values.storable.Values.stringValue;

public class IndexCursorFilterTest implements IndexProgressor, NodeValueClient
{
    @Rule
    public final MockStore store = new MockStore();
    private final List<Event> events = new ArrayList<>();

    @Test
    public void shouldAcceptAllNodesOnNoFilters() throws Exception
    {
        // given
        store.node( 17, NO_ID, false, NO_ID, 0 );
        NodeValueClientFilter filter = initializeFilter( null );

        // when
        assertTrue( filter.acceptNode( 17, null ) );
        filter.done();

        // then
        assertEvents( initialize(), new Event.Node( 17, null ), Event.DONE );
    }

    @Test
    public void shouldRejectNodeNotInUse() throws Exception
    {
        // given
        NodeValueClientFilter filter = initializeFilter( null, IndexQuery.exists( 12 ) );

        // when
        assertFalse( filter.acceptNode( 17, null ) );
        filter.done();

        // then
        assertEvents( initialize(), Event.DONE );
    }

    @Test
    public void shouldRejectNodeWithNoProperties() throws Exception
    {
        // given
        store.node( 17, NO_ID, false, NO_ID, 0 );
        NodeValueClientFilter filter = initializeFilter( null, IndexQuery.exists( 12 ) );

        // when
        assertFalse( filter.acceptNode( 17, null ) );
        filter.done();

        // then
        assertEvents( initialize(), Event.DONE );
    }

    @Test
    public void shouldAcceptNodeWithMatchingProperty() throws Exception
    {
        // given
        store.node( 17, 1, false, NO_ID, 0 );
        store.property( 1, NO_ID, NO_ID, block( 12, stringValue( "hello" ) ) );
        NodeValueClientFilter filter = initializeFilter( null, IndexQuery.exists( 12 ) );

        // when
        assertTrue( filter.acceptNode( 17, null ) );
        filter.done();

        // then
        assertEvents( initialize(), new Event.Node( 17, null ), Event.DONE );
    }

    @Test
    public void shouldNotAcceptNodeWithoutMatchingProperty() throws Exception
    {
        // given
        store.node( 17, 1, false, NO_ID, 0 );
        store.property( 1, NO_ID, NO_ID, block( 7, stringValue( "wrong" ) ) );
        NodeValueClientFilter filter = initializeFilter( null, IndexQuery.exists( 12 ) );

        // when
        assertFalse( filter.acceptNode( 17, null ) );
        filter.done();

        // then
        assertEvents( initialize(), Event.DONE );
    }

    private NodeValueClientFilter initializeFilter( int[] keys, IndexQuery... filters )
    {
        NodeValueClientFilter filter = new NodeValueClientFilter(
                this, new NodeCursor( store ), new PropertyCursor( store ), filters );
        filter.initialize( this, keys );
        return filter;
    }

    private void assertEvents( Event... expected )
    {
        assertEquals( Arrays.asList( expected ), events );
    }

    private Event.Initialize initialize( int... keys )
    {
        return new Event.Initialize( this, keys == null || keys.length == 0 ? null : keys );
    }

    @Override
    public void initialize( IndexProgressor progressor, int[] propertyIds )
    {
        events.add( new Event.Initialize( progressor, propertyIds ) );
    }

    @Override
    public void done()
    {
        events.add( Event.DONE );
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        events.add( new Event.Node( reference, values ) );
        return true;
    }

    @Override
    public boolean next()
    {
        throw new UnsupportedOperationException( "should not be called in these tests" );
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "should not be called in these tests" );
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
        }

        static final Event DONE = new Event()
        {
            @Override
            public String toString()
            {
                return "DONE";
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
        }

        @SuppressWarnings( "EqualsWhichDoesntCheckParameterClass" )
        @Override
        public final boolean equals( Object other )
        {
            return EqualsBuilder.reflectionEquals( this, other, true );
        }

        @Override
        public final int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this, false );
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
        }
    }
}
