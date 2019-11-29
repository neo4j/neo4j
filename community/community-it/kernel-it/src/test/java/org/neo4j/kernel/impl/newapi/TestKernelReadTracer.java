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
package org.neo4j.kernel.impl.newapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.neo4j.internal.kernel.api.KernelReadTracer;

import static org.assertj.core.api.Assertions.assertThat;

public class TestKernelReadTracer implements KernelReadTracer
{
    private final List<TraceEvent> traceEvents;

    TestKernelReadTracer()
    {
        traceEvents = new ArrayList<>();
    }

    @Override
    public void onNode( long nodeReference )
    {
        traceEvents.add( OnNode( nodeReference ) );
    }

    @Override
    public void onAllNodesScan()
    {
        traceEvents.add( ON_ALL_NODES_SCAN );
    }

    @Override
    public void onLabelScan( int label )
    {
        traceEvents.add( OnLabelScan( label ) );
    }

    @Override
    public void onIndexSeek()
    {
        traceEvents.add( OnIndexSeek() );
    }

    @Override
    public void onRelationship( long relationshipReference )
    {
        traceEvents.add( OnRelationship( relationshipReference ) );
    }

    @Override
    public void onRelationshipGroup( int type )
    {
        traceEvents.add( OnRelationshipGroup( type ) );
    }

    @Override
    public void onProperty( int propertyKey )
    {
        traceEvents.add( OnProperty( propertyKey ) );
    }

    void assertEvents( TraceEvent... expected )
    {
        assertEvents( Arrays.asList( expected ) );
    }

    void assertEvents( List<TraceEvent> expected )
    {
        assertThat( traceEvents ).isEqualTo( expected );
        clear();
    }

    void clear()
    {
        traceEvents.clear();
    }

    enum TraceEventKind
    {
        Node,
        AllNodesScan,
        LabelScan,
        IndexSeek,
        Relatioship,
        RelatioshipGroup,
        Property
    }

    static class TraceEvent
    {
        final TraceEventKind kind;
        final long hash;

        TraceEvent( TraceEventKind kind )
        {
            this( kind, 1 );
        }

        TraceEvent( TraceEventKind kind, long hash )
        {
            this.kind = kind;
            this.hash = hash;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            TraceEvent that = (TraceEvent) o;
            return hash == that.hash &&
                    kind == that.kind;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( kind, hash );
        }

        @Override
        public String toString()
        {
            return String.format( "%s[%d]", kind, hash );
        }
    }

    static final TraceEvent ON_ALL_NODES_SCAN = new TraceEvent( TraceEventKind.AllNodesScan );

    static TraceEvent OnNode( long nodeReference )
    {
        return new TraceEvent( TraceEventKind.Node, nodeReference );
    }

    static TraceEvent OnLabelScan( int label )
    {
        return new TraceEvent( TraceEventKind.LabelScan, label );
    }

    static TraceEvent OnIndexSeek()
    {
        return new TraceEvent( TraceEventKind.IndexSeek, 1 );
    }

    static TraceEvent OnRelationship( long relationshipReference )
    {
        return new TraceEvent( TraceEventKind.Relatioship, relationshipReference );
    }

    static TraceEvent OnRelationshipGroup( int type )
    {
        return new TraceEvent( TraceEventKind.RelatioshipGroup, type );
    }

    static TraceEvent OnProperty( int propertyKey )
    {
        return new TraceEvent( TraceEventKind.Property, propertyKey );
    }
}
