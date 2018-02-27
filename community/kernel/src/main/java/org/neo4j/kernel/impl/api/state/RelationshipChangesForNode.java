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
package org.neo4j.kernel.impl.api.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.LongConsumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongObjectMap;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.RelationshipVisitor.Home;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.newapi.RelationshipDirection;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;

/**
 * Maintains relationships that have been added for a specific node.
 * <p/>
 * This class is not a trustworthy source of information unless you are careful - it does not, for instance, remove
 * rels if they are added and then removed in the same tx. It trusts wrapping data structures for that filtering.
 */
public class RelationshipChangesForNode
{
    /**
     * Allows this data structure to work both for tracking removals and additions.
     */
    public enum DiffStrategy
    {
        REMOVE
                {
                    @Override
                    int augmentDegree( int degree, int diff )
                    {
                        return degree - diff;
                    }

                    @Override
                    RelationshipIterator augmentPrimitiveIterator( RelationshipIterator original,
                            Iterator<PrimitiveLongSet> diff, RelationshipVisitor.Home txStateRelationshipHome )
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    PrimitiveLongIterator getPrimitiveIterator( Iterator<PrimitiveLongSet> diff )
                    {
                        throw new UnsupportedOperationException();
                    }
                },
        ADD
                {
                    @Override
                    int augmentDegree( int degree, int diff )
                    {
                        return degree + diff;
                    }

                    @Override
                    RelationshipIterator augmentPrimitiveIterator( final RelationshipIterator original,
                            final Iterator<PrimitiveLongSet> diff, final RelationshipVisitor.Home txStateRelationshipHome )
                    {
                        if ( !diff.hasNext() )
                        {
                            return original;
                        }

                        return new RelationshipIterator()
                        {
                            private PrimitiveLongIterator currentSetOfAddedRels;

                            @Override
                            public boolean hasNext()
                            {
                                return original.hasNext() || (currentSetOfAddedRels().hasNext());
                            }

                            private PrimitiveLongIterator currentSetOfAddedRels()
                            {
                                while ( diff.hasNext() && (currentSetOfAddedRels == null || !currentSetOfAddedRels
                                        .hasNext()) )
                                {
                                    currentSetOfAddedRels = diff.next().iterator();
                                }
                                return currentSetOfAddedRels;
                            }

                            @Override
                            public long next()
                            {
                                return original.hasNext() ? original.next() : currentSetOfAddedRels().next();
                            }

                            @Override
                            public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                                    RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
                            {
                                RelationshipVisitor.Home home = currentSetOfAddedRels != null ?
                                        txStateRelationshipHome : original;
                                return home.relationshipVisit( relationshipId, visitor );
                            }
                        };
                    }

                    @Override
                    PrimitiveLongIterator getPrimitiveIterator( final Iterator<PrimitiveLongSet> diff )
                    {
                        if ( !diff.hasNext() )
                        {
                            return PrimitiveLongCollections.emptyIterator();
                        }

                        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
                        {
                            private PrimitiveLongIterator currentSetOfAddedRels;

                            @Override
                            protected boolean fetchNext()
                            {
                                PrimitiveLongIterator iterator = currentSetOfAddedRels();
                                return iterator.hasNext() && next( iterator.next() );
                            }

                            private PrimitiveLongIterator currentSetOfAddedRels()
                            {
                                while ( diff.hasNext() && (currentSetOfAddedRels == null || !currentSetOfAddedRels
                                        .hasNext()) )
                                {
                                    currentSetOfAddedRels = diff.next().iterator();
                                }
                                return currentSetOfAddedRels;
                            }
                        };
                    }
                };

        abstract int augmentDegree( int degree, int diff );

        abstract RelationshipIterator augmentPrimitiveIterator( RelationshipIterator original,
                Iterator<PrimitiveLongSet> diff, RelationshipVisitor.Home txStateRelationshipHome );

        abstract PrimitiveLongIterator getPrimitiveIterator( Iterator<PrimitiveLongSet> diff );
    }

    private final DiffStrategy diffStrategy;
    private final Home relationshipHome;

    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> outgoing;
    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> incoming;
    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> loops;

    private int totalOutgoing;
    private int totalIncoming;
    private int totalLoops;

    public RelationshipChangesForNode( DiffStrategy diffStrategy, RelationshipVisitor.Home relationshipHome )
    {
        this.diffStrategy = diffStrategy;
        this.relationshipHome = relationshipHome;
    }

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        VersionedPrimitiveLongObjectMap<PrimitiveLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        PrimitiveLongSet rels = relTypeToRelsMap.currentView().computeIfAbsent( typeId, k -> Primitive.longSet() );

        rels.add( relId );

        switch ( direction )
        {
            case INCOMING:
                totalIncoming++;
                break;
            case OUTGOING:
                totalOutgoing++;
                break;
            case BOTH:
                totalLoops++;
                break;
            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    public boolean removeRelationship( long relId, int typeId, Direction direction )
    {
        VersionedPrimitiveLongObjectMap<PrimitiveLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        PrimitiveLongSet rels = relTypeToRelsMap.currentView().get( typeId );
        if ( rels != null )
        {
            if ( rels.remove( relId ) )
            {
                if ( rels.isEmpty() )
                {
                    relTypeToRelsMap.currentView().remove( typeId );
                }

                switch ( direction )
                {
                    case INCOMING:
                        totalIncoming--;
                        break;
                    case OUTGOING:
                        totalOutgoing--;
                        break;
                    case BOTH:
                        totalLoops--;
                        break;
                    default:
                        throw new IllegalArgumentException( "Unknown direction: " + direction );
                }
                return true;
            }
        }
        return false;
    }

    public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels )
    {
        return augmentRelationships( direction, rels, ALL_TYPES );
    }

    public RelationshipIterator augmentRelationships( Direction direction, int[] types, RelationshipIterator rels )
    {
        return augmentRelationships( direction, rels, typeFilter( types ) );
    }

    public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels,
            Function<VersionedPrimitiveLongObjectMap<PrimitiveLongSet>, Iterator<PrimitiveLongSet>> typeFilter )
    {
        switch ( direction )
        {
            case INCOMING:
                if ( incoming != null && !incoming.currentView().isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( incoming ),
                            relationshipHome );
                }
                break;
            case OUTGOING:
                if ( outgoing != null && !outgoing.currentView().isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( outgoing ),
                            relationshipHome );
                }
                break;
            case BOTH:
                if ( outgoing != null && !outgoing.currentView().isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( outgoing ),
                            relationshipHome );
                }
                if ( incoming != null && !incoming.currentView().isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( incoming ),
                            relationshipHome );
                }
                break;

            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        // Loops are always included
        if ( loops != null && !loops.currentView().isEmpty() )
        {
            rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( loops ), relationshipHome );
        }

        return rels;
    }

    public int augmentDegree( Direction direction, int degree )
    {
        switch ( direction )
        {
            case INCOMING:
                return diffStrategy.augmentDegree( degree, totalIncoming + totalLoops );
            case OUTGOING:
                return diffStrategy.augmentDegree( degree, totalOutgoing + totalLoops );
            default:
                return diffStrategy.augmentDegree( degree, totalIncoming + totalOutgoing + totalLoops );
        }
    }

    public int augmentDegree( Direction direction, int degree, long typeId )
    {
        switch ( direction )
        {
            case INCOMING:
                if ( incoming != null && incoming.currentView().containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, incoming.currentView().get( typeId ).size() );
                }
                break;
            case OUTGOING:
                if ( outgoing != null && outgoing.currentView().containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, outgoing.currentView().get( typeId ).size() );
                }
                break;
            case BOTH:
                if ( outgoing != null && outgoing.currentView().containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, outgoing.currentView().get( typeId ).size() );
                }
                if ( incoming != null && incoming.currentView().containsKey( typeId ) )
                {
                    degree = diffStrategy.augmentDegree( degree, incoming.currentView().get( typeId ).size() );
                }
                break;

            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        // Loops are always included
        if ( loops != null && loops.currentView().containsKey( typeId ) )
        {
            degree = diffStrategy.augmentDegree( degree, loops.currentView().get( typeId ).size() );
        }
        return degree;
    }

    public int augmentDegree( RelationshipDirection direction, int degree, int typeId )
    {
        switch ( direction )
        {
        case INCOMING:
            if ( incoming != null && incoming.currentView().containsKey( typeId ) )
            {
                return diffStrategy.augmentDegree( degree, incoming.currentView().get( typeId ).size() );
            }
            break;
        case OUTGOING:
            if ( outgoing != null && outgoing.currentView().containsKey( typeId ) )
            {
                return diffStrategy.augmentDegree( degree, outgoing.currentView().get( typeId ).size() );
            }
            break;
        case LOOP:
            if ( loops != null && loops.currentView().containsKey( typeId ) )
            {
                return diffStrategy.augmentDegree( degree, loops.currentView().get( typeId ).size() );
            }
            break;

        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        return degree;
    }

    public PrimitiveIntSet relationshipTypes()
    {
        PrimitiveIntSet types = Primitive.intSet();
        LongConsumer typeCollector = value -> types.add( Math.toIntExact( value ) );
        if ( outgoing != null && !outgoing.currentView().isEmpty() )
        {
            outgoing.currentView().iterator().forEachRemaining( typeCollector );
        }
        if ( incoming != null && !incoming.currentView().isEmpty() )
        {
            incoming.currentView().iterator().forEachRemaining( typeCollector );
        }
        if ( loops != null && !loops.currentView().isEmpty() )
        {
            loops.currentView().iterator().forEachRemaining( typeCollector );
        }
        return types;
    }

    public void clear()
    {
        if ( outgoing != null )
        {
            outgoing.clear();
        }
        if ( incoming != null )
        {
            incoming.clear();
        }
        if ( loops != null )
        {
            loops.clear();
        }
    }

    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> outgoing()
    {
        if ( outgoing == null )
        {
            outgoing = new VersionedPrimitiveLongObjectMap<>();
        }
        return outgoing;
    }

    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> incoming()
    {
        if ( incoming == null )
        {
            incoming = new VersionedPrimitiveLongObjectMap<>();
        }
        return incoming;
    }

    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> loops()
    {
        if ( loops == null )
        {
            loops = new VersionedPrimitiveLongObjectMap<>();
        }
        return loops;
    }

    private VersionedPrimitiveLongObjectMap<PrimitiveLongSet> getTypeToRelMapForDirection( Direction direction )
    {
        VersionedPrimitiveLongObjectMap<PrimitiveLongSet> relTypeToRelsMap;
        switch ( direction )
        {
            case INCOMING:
                relTypeToRelsMap = incoming();
                break;
            case OUTGOING:
                relTypeToRelsMap = outgoing();
                break;
            case BOTH:
                relTypeToRelsMap = loops();
                break;
            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
        return relTypeToRelsMap;
    }

    private Function<VersionedPrimitiveLongObjectMap<PrimitiveLongSet>, Iterator<PrimitiveLongSet>> typeFilter( int[] types )
    {
        return relationshipsByType -> new PrefetchingIterator<PrimitiveLongSet>()
        {
            private final PrimitiveIntIterator iterTypes = PrimitiveIntCollections.iterator( types );

            @Override
            protected PrimitiveLongSet fetchNextOrNull()
            {
                while ( iterTypes.hasNext() )
                {
                    PrimitiveLongSet relsByType = relationshipsByType.currentView().get( iterTypes.next() );
                    if ( relsByType != null )
                    {
                        return relsByType;
                    }
                }
                return null;
            }
        };
    }

    private static final Function<VersionedPrimitiveLongObjectMap<PrimitiveLongSet>, Iterator<PrimitiveLongSet>> ALL_TYPES =
            integerSetMap -> integerSetMap.currentView().values().iterator();

    private Iterator<PrimitiveLongSet> diffs(
            Function<VersionedPrimitiveLongObjectMap<PrimitiveLongSet>, Iterator<PrimitiveLongSet>> filter,
            VersionedPrimitiveLongObjectMap<PrimitiveLongSet>... maps )
    {
        Collection<PrimitiveLongSet> result = new ArrayList<>();
        for ( VersionedPrimitiveLongObjectMap<PrimitiveLongSet> map : maps )
        {
            if ( map != null )
            {
                Iterator<PrimitiveLongSet> diffSet = filter.apply( map );
                while ( diffSet.hasNext() )
                {
                    result.add( diffSet.next() );
                }
            }
        }
        return result.iterator();
    }

    public PrimitiveLongIterator getRelationships( Direction direction )
    {
        return getRelationships( direction, ALL_TYPES );
    }

    // todo remove this if it's not used by previous cypher versions
    public PrimitiveLongIterator getRelationships( Direction direction, int[] types )
    {
        return getRelationships( direction, typeFilter( types ) );
    }

    public PrimitiveLongIterator getRelationships()
    {
        PrimitiveLongIterator longIterator = PrimitiveLongCollections.concat( primitiveIds( incoming ),
                primitiveIds( outgoing ), primitiveIds( loops ) );
        PrimitiveLongSet longSet = Primitive.longSet();
        while ( longIterator.hasNext() )
        {
            longSet.add( longIterator.next() );
        }
        return longSet.iterator();
    }

    public PrimitiveLongIterator getRelationships( RelationshipDirection direction, int type )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming != null ? primitiveIdsByType( incoming, type ) : emptyIterator();
        case OUTGOING:
            return outgoing != null ? primitiveIdsByType( outgoing, type ) : emptyIterator();
        case LOOP:
            return loops != null ? primitiveIdsByType( loops, type ) : emptyIterator();
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    private PrimitiveLongIterator primitiveIds( VersionedPrimitiveLongObjectMap<PrimitiveLongSet> map )
    {
        if ( map == null )
        {
            return emptyIterator();
        }
        PrimitiveLongObjectMap<PrimitiveLongSet> view = map.currentView();
        Iterable<PrimitiveLongSet> values = view.values();
        int size = view.size();
        PrimitiveLongIterator[] iterators = new PrimitiveLongIterator[size];
        int i = 0;
        for ( PrimitiveLongSet value : values )
        {
            iterators[i++] = value.iterator();
        }
        return PrimitiveLongCollections.concat( iterators );
    }

    private PrimitiveLongIterator primitiveIdsByType( VersionedPrimitiveLongObjectMap<PrimitiveLongSet> map, int type )
    {
        PrimitiveLongSet relationships = map.currentView().get( type );
        return relationships == null ? emptyIterator() : relationships.iterator();
    }

    private PrimitiveLongIterator getRelationships( Direction direction,
            Function<VersionedPrimitiveLongObjectMap<PrimitiveLongSet>, Iterator<PrimitiveLongSet>> types )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, incoming, loops ) ) : emptyIterator();
        case OUTGOING:
            return outgoing != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, outgoing, loops ) ) : emptyIterator();
        case BOTH:
            return outgoing != null || incoming != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, outgoing, incoming, loops ) ) : emptyIterator();
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }
}
