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
package org.neo4j.internal.kernel.api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This is an example of an abstraction on top of {@link RelationshipScanCursor} and {@link RelationshipGroupCursor}.
 */
public abstract class NeighbourCursor implements NodeCursor
{
    public static Builder outgoing( CursorFactory cursors, int label )
    {
        return new Builder( cursors ).outgoing( label );
    }

    public static Builder outgoing( CursorFactory cursors, int label, PropertyPredicate relationshipProperties )
    {
        return new Builder( cursors ).outgoing( label, relationshipProperties );
    }

    public static Builder incoming( CursorFactory cursors, int label )
    {
        return new Builder( cursors ).incoming( label );
    }

    public static Builder incoming( CursorFactory cursors, int label, PropertyPredicate relationshipProperties )
    {
        return new Builder( cursors ).incoming( label, relationshipProperties );
    }

    public static Builder any( CursorFactory cursors, int label )
    {
        return new Builder( cursors ).any( label );
    }

    public static Builder any( CursorFactory cursors, int label, PropertyPredicate relationshipProperties )
    {
        return new Builder( cursors ).any( label, relationshipProperties );
    }

    public static NeighbourCursor outgoing( CursorFactory cursors )
    {
        return new AnyLabel( cursors, true, false );
    }

    public static NeighbourCursor outgoing( CursorFactory cursors, PropertyPredicate relationshipProperties )
    {
        Objects.requireNonNull( relationshipProperties, "Relationship PropertyPredicate" );
        return new FilteringAnyLabel( cursors, true, false, relationshipProperties );
    }

    public static NeighbourCursor incoming( CursorFactory cursors )
    {
        return new AnyLabel( cursors, false, true );
    }

    public static NeighbourCursor incoming( CursorFactory cursors, PropertyPredicate relationshipProperties )
    {
        Objects.requireNonNull( relationshipProperties, "Relationship PropertyPredicate" );
        return new FilteringAnyLabel( cursors, false, true, relationshipProperties );
    }

    public static NeighbourCursor any( CursorFactory cursors )
    {
        return new AnyLabel( cursors, true, true );
    }

    public static NeighbourCursor any( CursorFactory cursors, PropertyPredicate relationshipProperties )
    {
        Objects.requireNonNull( relationshipProperties, "Relationship PropertyPredicate" );
        return new FilteringAnyLabel( cursors, true, true, relationshipProperties );
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

        public Builder outgoing( int label, PropertyPredicate relationshipProperties )
        {
            if ( Objects.requireNonNull( relationshipProperties, "Relationship PropertyPredicate" ) != NO_FILTER )
            {
                filterProperties = true;
            }
            alternatives.compute( label, ( key, entry ) ->
            {
                if ( entry == null )
                {
                    entry = new Entry( key );
                }
                entry.outgoing( relationshipProperties );
                return entry;
            } );
            return this;
        }

        public Builder incoming( int label )
        {
            return incoming( label, NO_FILTER );
        }

        public Builder incoming( int label, PropertyPredicate relationshipProperties )
        {
            if ( Objects.requireNonNull( relationshipProperties, "Relationship PropertyPredicate" ) != NO_FILTER )
            {
                filterProperties = true;
            }
            alternatives.compute( label, ( key, entry ) ->
            {
                if ( entry == null )
                {
                    entry = new Entry( key );
                }
                entry.incoming( relationshipProperties );
                return entry;
            } );
            return this;
        }

        public Builder any( int label )
        {
            return any( label, NO_FILTER );
        }

        public Builder any( int label, PropertyPredicate relationshipProperties )
        {
            if ( Objects.requireNonNull( relationshipProperties, "Relationship PropertyPredicate" ) != NO_FILTER )
            {
                filterProperties = true;
            }
            alternatives.compute( label, ( key, entry ) ->
            {
                if ( entry == null )
                {
                    entry = new Entry( key );
                }
                entry.any( relationshipProperties );
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
    private final RelationshipGroupCursor group;
    private final RelationshipTraversalCursor relationships;

    public NeighbourCursor( CursorFactory cursors )
    {
        this.neighbours = cursors.allocateNodeCursor();
        this.group = cursors.allocateRelationshipGroupCursor();
        this.relationships = cursors.allocateRelationshipTraversalCursor();
    }

    /**
     * Initialize this cursor to traverse the neighbours of the node currently under the specified cursor.
     *
     * @param node
     *         a cursor pointing at the node to get the neighbours for.
     */
    public final void of( NodeCursor node )
    {
        node.relationships( group );
        // make sure these don't have state from previous use
        group.close();
        relationships.close();
    }

    @Override
    public final boolean next()
    {
        while ( !relationships.next() )
        {
            if ( !next( group, relationships ) )
            {
                return false;
            }
        }
        do
        {
            relationships.neighbour( neighbours );
        }
        while ( relationships.shouldRetry() );
        return neighbours.next();
    }

    protected abstract boolean next( RelationshipGroupCursor group, RelationshipTraversalCursor relationships );

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
        relationships.close();
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
    public final void relationships( RelationshipGroupCursor cursor )
    {
        neighbours.relationships( cursor );
    }

    @Override
    public void outgoingRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {
        neighbours.outgoingRelationships( groups, relationships );
    }

    @Override
    public void incomingRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {
        neighbours.incomingRelationships( groups, relationships );
    }

    @Override
    public void allRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {
        neighbours.allRelationships( groups, relationships );
    }

    @Override
    public final void properties( PropertyCursor cursor )
    {
        neighbours.properties( cursor );
    }

    @Override
    public final long relationshipGroupReference()
    {
        return neighbours.relationshipGroupReference();
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
        protected boolean next( RelationshipGroupCursor group, RelationshipTraversalCursor relationships )
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
        protected boolean next( RelationshipGroupCursor group, RelationshipTraversalCursor relationships )
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
        protected boolean next( RelationshipGroupCursor group, RelationshipTraversalCursor relationships )
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
        protected boolean next( RelationshipGroupCursor group, RelationshipTraversalCursor relationships )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}
