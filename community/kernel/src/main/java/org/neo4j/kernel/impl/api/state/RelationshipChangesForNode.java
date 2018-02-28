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
import java.util.function.BiFunction;
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
import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongSet;
import org.neo4j.helpers.collection.Iterators;
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
    private static final BiFunction<VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet>,StateSelector,Iterator<PrimitiveLongSet>>
            ALL_TYPES = ( integerSetMap, stateSelector ) -> Iterators
            .map( stateSelector::getView, stateSelector.getView( integerSetMap ).values().iterator() );

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

    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> outgoing;
    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> incoming;
    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> loops;

    private int totalOutgoing;
    private int totalIncoming;
    private int totalLoops;

    public RelationshipChangesForNode( DiffStrategy diffStrategy, RelationshipVisitor.Home relationshipHome )
    {
        this.diffStrategy = diffStrategy;
        this.relationshipHome = relationshipHome;
    }

    void markStable()
    {
        if ( outgoing != null )
        {
            outgoing.markStable();
            outgoing.stableView().values().forEach( VersionedPrimitiveLongSet::markStable );
        }
        if ( incoming != null )
        {
            incoming.markStable();
            incoming.stableView().values().forEach( VersionedPrimitiveLongSet::markStable );
        }
        if ( loops != null )
        {
            loops.markStable();
            loops.stableView().values().forEach( VersionedPrimitiveLongSet::markStable );
        }
    }

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        VersionedPrimitiveLongSet rels = relTypeToRelsMap.currentView().computeIfAbsent( typeId, k -> new VersionedPrimitiveLongSet() );

        rels.currentView().add( relId );

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
        VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> relTypeToRelsMap = getTypeToRelMapForDirection( direction );
        PrimitiveLongObjectMap<VersionedPrimitiveLongSet> typeToRelationshipsView = relTypeToRelsMap.currentView();
        VersionedPrimitiveLongSet rels = typeToRelationshipsView.get( typeId );
        if ( rels != null )
        {
            PrimitiveLongSet relationshipView = rels.currentView();
            if ( relationshipView.remove( relId ) )
            {
                if ( relationshipView.isEmpty() )
                {
                    typeToRelationshipsView.remove( typeId );
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

    public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels, StateSelector stateSelector )
    {
        return augmentRelationships( direction, rels, ALL_TYPES, stateSelector );
    }

    public RelationshipIterator augmentRelationships( Direction direction, int[] types, RelationshipIterator rels,
            StateSelector stateSelector )
    {
        return augmentRelationships( direction, rels, typeFilter( types ), stateSelector );
    }

    private RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels,
            BiFunction<VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet>, StateSelector,
                    Iterator<PrimitiveLongSet>> typeFilter, StateSelector stateSelector )
    {
        switch ( direction )
        {
            case INCOMING:
                if ( incoming != null && !stateSelector.getView( incoming ).isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( incoming, stateSelector ),
                            relationshipHome );
                }
                break;
            case OUTGOING:
                if ( outgoing != null && !stateSelector.getView( outgoing ).isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( outgoing, stateSelector ),
                            relationshipHome );
                }
                break;
            case BOTH:
                if ( outgoing != null && !stateSelector.getView( outgoing ).isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( outgoing, stateSelector ),
                            relationshipHome );
                }
                if ( incoming != null && !stateSelector.getView( incoming ).isEmpty() )
                {
                    rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( incoming, stateSelector ),
                            relationshipHome );
                }
                break;

            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        // Loops are always included
        if ( loops != null && !stateSelector.getView( loops ).isEmpty() )
        {
            rels = diffStrategy.augmentPrimitiveIterator( rels, typeFilter.apply( loops, stateSelector ), relationshipHome );
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

    public int augmentDegree( Direction direction, int degree, long typeId, StateSelector stateSelector )
    {
        switch ( direction )
        {
            case INCOMING:
                if ( incoming != null && stateSelector.getView( incoming ).containsKey( typeId ) )
                {
                    VersionedPrimitiveLongSet set = stateSelector.getView( incoming ).get( typeId );
                    degree = diffStrategy.augmentDegree( degree, stateSelector.getView( set ).size() );
                }
                break;
            case OUTGOING:
                if ( outgoing != null && stateSelector.getView( outgoing ).containsKey( typeId ) )
                {
                    VersionedPrimitiveLongSet set = stateSelector.getView( outgoing ).get( typeId );
                    degree = diffStrategy.augmentDegree( degree, stateSelector.getView( set ).size() );
                }
                break;
            case BOTH:
                if ( outgoing != null && stateSelector.getView( outgoing ).containsKey( typeId ) )
                {
                    VersionedPrimitiveLongSet set = stateSelector.getView( outgoing ).get( typeId );
                    degree = diffStrategy.augmentDegree( degree, stateSelector.getView( set ).size() );
                }
                if ( incoming != null && stateSelector.getView( incoming ).containsKey( typeId ) )
                {
                    VersionedPrimitiveLongSet set = stateSelector.getView( incoming ).get( typeId );
                    degree = diffStrategy.augmentDegree( degree, stateSelector.getView( set ).size() );
                }
                break;

            default:
                throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        // Loops are always included
        if ( loops != null && stateSelector.getView( loops ).containsKey( typeId ) )
        {
            VersionedPrimitiveLongSet set = stateSelector.getView( loops ).get( typeId );
            degree = diffStrategy.augmentDegree( degree, stateSelector.getView( set ).size() );
        }
        return degree;
    }

    public int augmentDegree( RelationshipDirection direction, int degree, int typeId, StateSelector stateSelector )
    {
        switch ( direction )
        {
        case INCOMING:
            if ( incoming != null && stateSelector.getView( incoming ).containsKey( typeId ) )
            {
                PrimitiveLongObjectMap<VersionedPrimitiveLongSet> map = stateSelector.getView( incoming );
                return diffStrategy.augmentDegree( degree, stateSelector.getView( map.get( typeId ) ).size() );
            }
            break;
        case OUTGOING:
            if ( outgoing != null && stateSelector.getView( outgoing ).containsKey( typeId ) )
            {
                PrimitiveLongObjectMap<VersionedPrimitiveLongSet> map = stateSelector.getView( outgoing );
                return diffStrategy.augmentDegree( degree, stateSelector.getView( map.get( typeId ) ).size() );
            }
            break;
        case LOOP:
            if ( loops != null && stateSelector.getView( loops ).containsKey( typeId ) )
            {
                PrimitiveLongObjectMap<VersionedPrimitiveLongSet> map = stateSelector.getView( loops );
                return diffStrategy.augmentDegree( degree, stateSelector.getView( map.get( typeId ) ).size() );
            }
            break;

        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }

        return degree;
    }

    public PrimitiveIntSet relationshipTypes( StateSelector stateSelector )
    {
        PrimitiveIntSet types = Primitive.intSet();
        LongConsumer typeCollector = value -> types.add( Math.toIntExact( value ) );
        if ( outgoing != null && !stateSelector.getView( outgoing ).isEmpty() )
        {
            stateSelector.getView( outgoing ).iterator().forEachRemaining( typeCollector );
        }
        if ( incoming != null && !stateSelector.getView( incoming ).isEmpty() )
        {
            stateSelector.getView( incoming ).iterator().forEachRemaining( typeCollector );
        }
        if ( loops != null && !stateSelector.getView( loops ).isEmpty() )
        {
            stateSelector.getView( loops ).iterator().forEachRemaining( typeCollector );
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

    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> outgoing()
    {
        if ( outgoing == null )
        {
            outgoing = new VersionedPrimitiveLongObjectMap<>();
        }
        return outgoing;
    }

    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> incoming()
    {
        if ( incoming == null )
        {
            incoming = new VersionedPrimitiveLongObjectMap<>();
        }
        return incoming;
    }

    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> loops()
    {
        if ( loops == null )
        {
            loops = new VersionedPrimitiveLongObjectMap<>();
        }
        return loops;
    }

    private VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> getTypeToRelMapForDirection( Direction direction )
    {
        VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> relTypeToRelsMap;
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

    private BiFunction<VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet>,StateSelector,Iterator<PrimitiveLongSet>> typeFilter(
            int[] types )
    {
        return ( relationshipsByType, selector ) -> new PrefetchingIterator<PrimitiveLongSet>()
        {
            private final PrimitiveIntIterator iterTypes = PrimitiveIntCollections.iterator( types );

            @Override
            protected PrimitiveLongSet fetchNextOrNull()
            {
                while ( iterTypes.hasNext() )
                {
                    VersionedPrimitiveLongSet relsByType =
                            selector.getView( relationshipsByType ).get( iterTypes.next() );
                    if ( relsByType != null )
                    {
                        return selector.getView( relsByType );
                    }
                }
                return null;
            }
        };
    }

    private Iterator<PrimitiveLongSet> diffs(
            BiFunction<VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet>,StateSelector, Iterator<PrimitiveLongSet>> filter,
            StateSelector stateSelector,
            VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet>... maps )
    {
        Collection<PrimitiveLongSet> result = new ArrayList<>();
        for ( VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> map : maps )
        {
            if ( map != null )
            {
                Iterator<PrimitiveLongSet> diffSet = filter.apply( map, stateSelector );
                while ( diffSet.hasNext() )
                {
                    result.add( diffSet.next() );
                }
            }
        }
        return result.iterator();
    }

    public PrimitiveLongIterator getRelationships( Direction direction, StateSelector stateSelector )
    {
        return getRelationships( direction, ALL_TYPES, stateSelector );
    }

    // todo remove this if it's not used by previous cypher versions
    public PrimitiveLongIterator getRelationships( Direction direction, int[] types, StateSelector stateSelector )
    {
        return getRelationships( direction, typeFilter( types ), stateSelector );
    }

    public PrimitiveLongIterator getRelationships( StateSelector stateSelector )
    {
        PrimitiveLongIterator longIterator = PrimitiveLongCollections.concat( primitiveIds( incoming, stateSelector ),
                primitiveIds( outgoing, stateSelector ), primitiveIds( loops, stateSelector ) );
        PrimitiveLongSet longSet = Primitive.longSet();
        while ( longIterator.hasNext() )
        {
            longSet.add( longIterator.next() );
        }
        return longSet.iterator();
    }

    public PrimitiveLongIterator getRelationships( RelationshipDirection direction, int type, StateSelector stateSelector )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming != null ? primitiveIdsByType( incoming, type, stateSelector ) : emptyIterator();
        case OUTGOING:
            return outgoing != null ? primitiveIdsByType( outgoing, type, stateSelector ) : emptyIterator();
        case LOOP:
            return loops != null ? primitiveIdsByType( loops, type, stateSelector ) : emptyIterator();
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }

    private PrimitiveLongIterator primitiveIds( VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> map,
            StateSelector stateSelector )
    {
        if ( map == null )
        {
            return emptyIterator();
        }
        PrimitiveLongObjectMap<VersionedPrimitiveLongSet> view = stateSelector.getView( map );
        Iterable<VersionedPrimitiveLongSet> values = view.values();
        int size = view.size();
        PrimitiveLongIterator[] iterators = new PrimitiveLongIterator[size];
        int i = 0;
        for ( VersionedPrimitiveLongSet value : values )
        {
            iterators[i++] = stateSelector.getView( value ).iterator();
        }
        return PrimitiveLongCollections.concat( iterators );
    }

    private PrimitiveLongIterator primitiveIdsByType( VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet> map,
            int type, StateSelector stateSelector )
    {
        VersionedPrimitiveLongSet relationships = stateSelector.getView( map ).get( type );
        return relationships == null ? emptyIterator() : stateSelector.getView( relationships ).iterator();
    }

    private PrimitiveLongIterator getRelationships( Direction direction,
            BiFunction<VersionedPrimitiveLongObjectMap<VersionedPrimitiveLongSet>,StateSelector,Iterator<PrimitiveLongSet>> types,
            StateSelector stateSelector )
    {
        switch ( direction )
        {
        case INCOMING:
            return incoming != null || loops != null ? diffStrategy.getPrimitiveIterator(
                    diffs( types, stateSelector, incoming, loops ) ) : emptyIterator();
        case OUTGOING:
            return outgoing != null || loops != null ? diffStrategy
                    .getPrimitiveIterator( diffs( types, stateSelector, outgoing, loops ) ) : emptyIterator();
        case BOTH:
            return outgoing != null || incoming != null || loops != null ? diffStrategy
                    .getPrimitiveIterator( diffs( types, stateSelector, outgoing, incoming, loops ) ) : emptyIterator();
        default:
            throw new IllegalArgumentException( "Unknown direction: " + direction );
        }
    }
}
