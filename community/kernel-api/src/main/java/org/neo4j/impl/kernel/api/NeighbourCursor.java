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
package org.neo4j.impl.kernel.api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.impl.kernel.api.result.ValueWriter;

/**
 * This is an example of an abstraction on top of {@link EdgeScanCursor} and {@link EdgeGroupCursor}.
 */
public abstract class NeighbourCursor implements NodeCursor
{
    public static Builder outgoing( CursorFactory cursors, int label )
    {
        return new Builder( cursors ).outgoing( label );
    }

    public static Builder outgoing( CursorFactory cursors, int label, PropertyPredicate edgeProperties )
    {
        return new Builder( cursors ).outgoing( label, edgeProperties );
    }

    public static Builder incoming( CursorFactory cursors, int label )
    {
        return new Builder( cursors ).incoming( label );
    }

    public static Builder incoming( CursorFactory cursors, int label, PropertyPredicate edgeProperties )
    {
        return new Builder( cursors ).incoming( label, edgeProperties );
    }

    public static Builder any( CursorFactory cursors, int label )
    {
        return new Builder( cursors ).any( label );
    }

    public static Builder any( CursorFactory cursors, int label, PropertyPredicate edgeProperties )
    {
        return new Builder( cursors ).any( label, edgeProperties );
    }

    public static NeighbourCursor outgoing( CursorFactory cursors )
    {
        return new AnyLabel( cursors, true, false );
    }

    public static NeighbourCursor outgoing( CursorFactory cursors, PropertyPredicate edgeProperties )
    {
        Objects.requireNonNull( edgeProperties, "Edge PropertyPredicate" );
        return new FilteringAnyLabel( cursors, true, false, edgeProperties );
    }

    public static NeighbourCursor incoming( CursorFactory cursors )
    {
        return new AnyLabel( cursors, false, true );
    }

    public static NeighbourCursor incoming( CursorFactory cursors, PropertyPredicate edgeProperties )
    {
        Objects.requireNonNull( edgeProperties, "Edge PropertyPredicate" );
        return new FilteringAnyLabel( cursors, false, true, edgeProperties );
    }

    public static NeighbourCursor any( CursorFactory cursors )
    {
        return new AnyLabel( cursors, true, true );
    }

    public static NeighbourCursor any( CursorFactory cursors, PropertyPredicate edgeProperties )
    {
        Objects.requireNonNull( edgeProperties, "Edge PropertyPredicate" );
        return new FilteringAnyLabel( cursors, true, true, edgeProperties );
    }

    public static final class Builder
    {
        public NeighbourCursor build()
        {
            Entry[] entries = alternatives.values().toArray( OF_ENTRIES );
            Arrays.sort( entries );
            if ( filterProperties )
            {
                return new FilteringExplicitLabels( cursors, entries );
            }
            else
            {
                return new ExplicitLabels( cursors, entries );
            }
        }

        public Builder outgoing( int label )
        {
            return outgoing( label, NO_FILTER );
        }

        public Builder outgoing( int label, PropertyPredicate edgeProperties )
        {
            if ( Objects.requireNonNull( edgeProperties, "Edge PropertyPredicate" ) != NO_FILTER )
            {
                filterProperties = true;
            }
            alternatives.compute( label, ( key, entry ) ->
            {
                if ( entry == null )
                {
                    entry = new Entry( key );
                }
                entry.outgoing( edgeProperties );
                return entry;
            } );
            return this;
        }

        public Builder incoming( int label )
        {
            return incoming( label, NO_FILTER );
        }

        public Builder incoming( int label, PropertyPredicate edgeProperties )
        {
            if ( Objects.requireNonNull( edgeProperties, "Edge PropertyPredicate" ) != NO_FILTER )
            {
                filterProperties = true;
            }
            alternatives.compute( label, ( key, entry ) ->
            {
                if ( entry == null )
                {
                    entry = new Entry( key );
                }
                entry.incoming( edgeProperties );
                return entry;
            } );
            return this;
        }

        public Builder any( int label )
        {
            return any( label, NO_FILTER );
        }

        public Builder any( int label, PropertyPredicate edgeProperties )
        {
            if ( Objects.requireNonNull( edgeProperties, "Edge PropertyPredicate" ) != NO_FILTER )
            {
                filterProperties = true;
            }
            alternatives.compute( label, ( key, entry ) ->
            {
                if ( entry == null )
                {
                    entry = new Entry( key );
                }
                entry.any( edgeProperties );
                return entry;
            } );
            return this;
        }

        private static final Entry[] OF_ENTRIES = {};
        private static final PropertyPredicate NO_FILTER = new PropertyPredicate()
        {
        };
        private final Map<Integer,Entry> alternatives = new HashMap<>();
        private boolean filterProperties;
        private final CursorFactory cursors;

        private Builder( CursorFactory cursors )
        {
            this.cursors = cursors;
        }

        private static final class Entry implements Comparable<Entry>
        {
            final int label;
            PropertyPredicate incoming, outgoing, any;

            Entry( int label )
            {
                this.label = label;
            }

            @Override
            public int compareTo( Entry that )
            {
                return this.label - that.label;
            }

            void outgoing( PropertyPredicate predicate )
            {
                if ( any != null || outgoing != null )
                {
                    throw new IllegalStateException( "Already specified" );
                }
                if ( incoming == predicate )
                {
                    any = predicate;
                    incoming = null;
                }
                else
                {
                    outgoing = predicate;
                }
            }

            void incoming( PropertyPredicate predicate )
            {
                if ( any != null || incoming != null )
                {
                    throw new IllegalStateException( "Already specified" );
                }
                if ( outgoing == predicate )
                {
                    any = predicate;
                    outgoing = null;
                }
                else
                {
                    incoming = predicate;
                }
            }

            void any( PropertyPredicate predicate )
            {
                if ( any != null || outgoing != null || incoming != null )
                {
                    throw new IllegalStateException( "Already specified" );
                }
                any = predicate;
            }
        }
    }

    private final NodeCursor neighbours;
    private final EdgeGroupCursor group;
    private final EdgeTraversalCursor edges;

    public NeighbourCursor( CursorFactory cursors )
    {
        this.neighbours = cursors.allocateNodeCursor();
        this.group = cursors.allocateEdgeGroupCursor();
        this.edges = cursors.allocateEdgeTraversalCursor();
    }

    /**
     * Initialize this cursor to traverse the neighbours of the node currently under the specified cursor.
     *
     * @param node
     *         a cursor pointing at the node to get the neighbours for.
     */
    public final void of( NodeCursor node )
    {
        node.edges( group );
        // make sure these don't have state from previous use
        group.close();
        edges.close();
    }

    @Override
    public final boolean next()
    {
        while ( !edges.next() )
        {
            if ( !next( group, edges ) )
            {
                return false;
            }
        }
        do
        {
            edges.neighbour( neighbours );
        }
        while ( edges.shouldRetry() );
        return neighbours.next();
    }

    @Override
    public void writeIdTo( ValueWriter target )
    {
        neighbours.writeIdTo( target );
    }

    protected abstract boolean next( EdgeGroupCursor group, EdgeTraversalCursor edges );

    @Override
    public final boolean shouldRetry()
    {
        return neighbours.shouldRetry();
    }

    @Override
    public final void close()
    {
        neighbours.close();
        group.close();
        edges.close();
    }

    @Override
    public final long nodeReference()
    {
        return neighbours.nodeReference();
    }

    @Override
    public final LabelSet labels()
    {
        return neighbours.labels();
    }

    @Override
    public final boolean hasProperties()
    {
        return neighbours.hasProperties();
    }

    @Override
    public final void edges( EdgeGroupCursor cursor )
    {
        neighbours.edges( cursor );
    }

    @Override
    public void outgoingEdges( EdgeGroupCursor groups, EdgeTraversalCursor edges )
    {
        neighbours.outgoingEdges( groups, edges );
    }

    @Override
    public void incomingEdges( EdgeGroupCursor groups, EdgeTraversalCursor edges )
    {
        neighbours.incomingEdges( groups, edges );
    }

    @Override
    public void allEdges( EdgeGroupCursor groups, EdgeTraversalCursor edges )
    {
        neighbours.allEdges( groups, edges );
    }

    @Override
    public final void properties( PropertyCursor cursor )
    {
        neighbours.properties( cursor );
    }

    @Override
    public final long edgeGroupReference()
    {
        return neighbours.edgeGroupReference();
    }

    @Override
    public final long propertiesReference()
    {
        return neighbours.propertiesReference();
    }

    private static final class AnyLabel extends NeighbourCursor
    {
        private final boolean outgoing;
        private final boolean incoming;

        AnyLabel( CursorFactory cursors, boolean outgoing, boolean incoming )
        {
            super( cursors );
            this.outgoing = outgoing;
            this.incoming = incoming;
        }

        @Override
        protected boolean next( EdgeGroupCursor group, EdgeTraversalCursor edges )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    private static final class FilteringAnyLabel extends NeighbourCursor
    {
        private final PropertyCursor properties;
        private final boolean outgoing;
        private final boolean incoming;
        private final PropertyPredicate predicate;

        FilteringAnyLabel( CursorFactory cursors, boolean outgoing, boolean incoming, PropertyPredicate predicate )
        {
            super( cursors );
            this.properties = cursors.allocatePropertyCursor();
            this.outgoing = outgoing;
            this.incoming = incoming;
            this.predicate = predicate;
        }

        @Override
        protected boolean next( EdgeGroupCursor group, EdgeTraversalCursor edges )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    private static final class ExplicitLabels extends NeighbourCursor
    {
        ExplicitLabels( CursorFactory cursors, Builder.Entry[] spec )
        {
            super( cursors );
        }

        @Override
        protected boolean next( EdgeGroupCursor group, EdgeTraversalCursor edges )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    private static final class FilteringExplicitLabels extends NeighbourCursor
    {
        private final PropertyCursor properties;

        FilteringExplicitLabels( CursorFactory cursors, Builder.Entry[] spec )
        {
            super( cursors );
            this.properties = cursors.allocatePropertyCursor();
        }

        @Override
        protected boolean next( EdgeGroupCursor group, EdgeTraversalCursor edges )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}
