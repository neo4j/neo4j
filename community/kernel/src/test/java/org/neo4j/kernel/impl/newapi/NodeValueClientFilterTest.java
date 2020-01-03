/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

public class NodeValueClientFilterTest implements IndexProgressor, NodeValueClient
{
    @Rule
    public final RandomRule random = new RandomRule();

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

    @Test
    public void shouldConsultProvidedAcceptingFiltersForMixOfValuesAndNoValues()
    {
        shouldConsultProvidedFilters( Function.identity(), true );
    }

    @Test
    public void shouldConsultProvidedAcceptingFiltersForNullValues()
    {
        shouldConsultProvidedFilters( v -> null, true );
    }

    @Test
    public void shouldConsultProvidedDenyingFiltersForMixOfValuesAndNoValues()
    {
        shouldConsultProvidedFilters( Function.identity(), false );
    }

    @Test
    public void shouldConsultProvidedDenyingFiltersForNullValues()
    {
        shouldConsultProvidedFilters( v -> null, false );
    }

    private void shouldConsultProvidedFilters( Function<Value[],Value[]> filterValues, boolean filterAcceptsValue )
    {
        // given
        long nodeReference = 123;
        int labelId = 10;
        int slots = random.nextInt( 3, 8 );
        IndexQuery[] filters = new IndexQuery[slots];
        Value[] actualValues = new Value[slots];
        Value[] values = new Value[slots];
        Map<Integer,Value> properties = new HashMap<>();
        int[] propertyKeyIds = new int[slots];
        int filterCount = 0;
        for ( int i = 0; i < slots; i++ )
        {
            actualValues[i] = random.nextValue();
            int propertyKeyId = i;
            propertyKeyIds[i] = propertyKeyId;
            //    we want at least one filter         ,  randomly add filter or not
            if ( (filterCount == 0 && i == slots - 1) || random.nextBoolean() )
            {
                Object filterValue = (filterAcceptsValue ? actualValues[i] : anyOtherValueThan( actualValues[i] )).asObjectCopy();
                filters[i] = IndexQuery.exact( propertyKeyId, filterValue );
                filterCount++;
            }
            values[i] = random.nextBoolean() ? NO_VALUE : actualValues[i];
            properties.put( propertyKeyId, actualValues[i] );
        }
        node.withNode( nodeReference, new long[]{labelId}, properties );

        // when
        NodeValueClientFilter filter = new NodeValueClientFilter( this, node, property, read, filters );
        filter.initialize( TestIndexDescriptorFactory.forLabel( labelId, propertyKeyIds ), this, null, IndexOrder.NONE, true );
        boolean accepted = filter.acceptNode( nodeReference, filterValues.apply( values ) );

        // then
        assertEquals( filterAcceptsValue, accepted );
    }

    private Value anyOtherValueThan( Value valueToNotReturn )
    {
        Value candidate;
        do
        {
            candidate = random.nextValue();
        }
        while ( candidate.eq( valueToNotReturn ) );
        return candidate;
    }

    private NodeValueClientFilter initializeFilter( IndexQuery... filters )
    {
        NodeValueClientFilter filter = new NodeValueClientFilter(
                this, node, property, read, filters );
        filter.initialize( TestIndexDescriptorFactory.forLabel( 11), this, null, IndexOrder.NONE, true );
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
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] queries, IndexOrder indexOrder, boolean needsValues )
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
