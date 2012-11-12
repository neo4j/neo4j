/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import static java.util.Arrays.asList;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.helpers.collection.NestingIterator;

public abstract class StandardExpander implements Expander
{
    private StandardExpander()
    {
    }

    static abstract class StandardExpansion<T> implements Expansion<T>
    {
        final StandardExpander expander;
        final Node start;

        StandardExpansion( StandardExpander expander, Node start )
        {
            this.expander = expander;
            this.start = start;
        }

        String stringRepresentation( String nodesORrelationships )
        {
            return "Expansion[" + start + ".expand( " + expander + " )."
                   + nodesORrelationships + "()]";
        }

        abstract StandardExpansion<T> createNew(
                @SuppressWarnings( "hiding" ) StandardExpander expander );

        public StandardExpansion<T> including( RelationshipType type )
        {
            return createNew( expander.add( type ) );
        }

        public StandardExpansion<T> including( RelationshipType type,
                Direction direction )
        {
            return createNew( expander.add( type, direction ) );
        }

        public StandardExpansion<T> excluding( RelationshipType type )
        {
            return createNew( expander.remove( type ) );
        }

        public StandardExpander expander()
        {
            return expander;
        }

        public StandardExpansion<T> filterNodes( Predicate<? super Node> filter )
        {
            return createNew( expander.addNodeFilter( filter ) );
        }

        public StandardExpansion<T> filterRelationships(
                Predicate<? super Relationship> filter )
        {
            return createNew( expander.addRelationshipFilter( filter ) );
        }

        public T getSingle()
        {
            final Iterator<T> expanded = iterator();
            if ( expanded.hasNext() )
            {
                final T result = expanded.next();
                if ( expanded.hasNext() )
                {
                    throw new NotFoundException(
                            "More than one relationship found for " + this );
                }
                return result;
            }
            return null;
        }

        public boolean isEmpty()
        {
            return !expander.doExpand( start ).hasNext();
        }

        public StandardExpansion<Node> nodes()
        {
            return new NodeExpansion( expander, start );
        }

        public StandardExpansion<Relationship> relationships()
        {
            return new RelationshipExpansion( expander, start );
        }

        public StandardExpansion<Pair<Relationship, Node>> pairs()
        {
            return new PairExpansion( expander, start );
        }
    }

    private static final class RelationshipExpansion extends
            StandardExpansion<Relationship>
    {
        RelationshipExpansion( StandardExpander expander, Node start )
        {
            super( expander, start );
        }

        @Override
        public String toString()
        {
            return stringRepresentation( "relationships" );
        }

        @Override
        StandardExpansion<Relationship> createNew(
                @SuppressWarnings( "hiding" ) StandardExpander expander )
        {
            return new RelationshipExpansion( expander, start );
        }

        @Override
        public StandardExpansion<Relationship> relationships()
        {
            return this;
        }

        public Iterator<Relationship> iterator()
        {
            return expander.doExpand( start );
        }
    }

    private static final class NodeExpansion extends StandardExpansion<Node>
    {
        NodeExpansion( StandardExpander expander, Node start )
        {
            super( expander, start );
        }

        @Override
        public String toString()
        {
            return stringRepresentation( "nodes" );
        }

        @Override
        StandardExpansion<Node> createNew(
                @SuppressWarnings( "hiding" ) StandardExpander expander )
        {
            return new NodeExpansion( expander, start );
        }

        @Override
        public StandardExpansion<Node> nodes()
        {
            return this;
        }

        public Iterator<Node> iterator()
        {
            return new IteratorWrapper<Node, Relationship>(
                    expander.doExpand( start ) )
            {
                @Override
                protected Node underlyingObjectToObject( Relationship rel )
                {
                    return rel.getOtherNode( start );
                }
            };
        }
    }

    private static final class PairExpansion extends
            StandardExpansion<Pair<Relationship, Node>>
    {
        PairExpansion( StandardExpander expander, Node start )
        {
            super( expander, start );
        }

        @Override
        public String toString()
        {
            return stringRepresentation( "pairs" );
        }

        @Override
        StandardExpansion<Pair<Relationship, Node>> createNew(
                @SuppressWarnings( "hiding" ) StandardExpander expander )
        {
            return new PairExpansion( expander, start );
        }

        @Override
        public StandardExpansion<Pair<Relationship, Node>> pairs()
        {
            return this;
        }

        public Iterator<Pair<Relationship, Node>> iterator()
        {
            return new IteratorWrapper<Pair<Relationship, Node>, Relationship>(
                    expander.doExpand( start ) )
            {
                @Override
                protected Pair<Relationship, Node> underlyingObjectToObject(
                        Relationship rel )
                {
                    return Pair.of( rel, rel.getOtherNode( start ) );
                }
            };
        }
    }

    private static class AllExpander extends StandardExpander
    {
        private final Direction direction;

        AllExpander( Direction direction )
        {
            this.direction = direction;
        }

        @Override
        void buildString( StringBuilder result )
        {
            if ( direction != Direction.BOTH )
            {
                result.append( direction );
                result.append( ":" );
            }
            result.append( "*" );
        }

        @Override
        Iterator<Relationship> doExpand( Node start )
        {
            return direction == Direction.BOTH ?
                    start.getRelationships().iterator() :
                    start.getRelationships( direction ).iterator();
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction dir )
        {
            return this;
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            Map<String, Exclusion> exclude = new HashMap<String, Exclusion>();
            exclude.put( type.name(), Exclusion.ALL );
            return new ExcludingExpander( Exclusion.include( direction ),
                    exclude );
        }

        @Override
        public StandardExpander reversed()
        {
            return new AllExpander( direction.reverse() );
        }
    }

    private enum Exclusion
    {
        ALL( null, "!" )
        {
            @Override
            public boolean accept( Node start, Relationship rel )
            {
                return false;
            }
        },
        INCOMING( Direction.OUTGOING )
        {
            @Override
            Exclusion reversed()
            {
                return OUTGOING;
            }
        },
        OUTGOING( Direction.INCOMING )
        {
            @Override
            Exclusion reversed()
            {
                return INCOMING;
            }
        },
        NONE( Direction.BOTH, "" )
        {
            @Override
            boolean includes( Direction direction )
            {
                return true;
            }
        };
        private final String string;
        private final Direction direction;

        private Exclusion( Direction direction, String string )
        {
            this.direction = direction;
            this.string = string;
        }

        private Exclusion( Direction direction )
        {
            this.direction = direction;
            this.string = "!" + name() + ":";
        }

        @Override
        public final String toString()
        {
            return string;
        }

        boolean accept( Node start, Relationship rel )
        {
            return matchDirection( direction, start, rel );
        }

        Exclusion reversed()
        {
            return this;
        }

        boolean includes( Direction dir )
        {
            return this.direction == dir;
        }

        static Exclusion include( Direction direction )
        {
            switch ( direction )
            {
            case INCOMING:
                return OUTGOING;
            case OUTGOING:
                return INCOMING;
            default:
                return NONE;
            }
        }
    }

    private static final class ExcludingExpander extends StandardExpander
    {
        private final Exclusion defaultExclusion;
        private final Map<String, Exclusion> exclusion;

        ExcludingExpander( Exclusion defaultExclusion,
                Map<String, Exclusion> exclusion )
        {
            this.defaultExclusion = defaultExclusion;
            this.exclusion = exclusion;
        }

        @Override
        void buildString( StringBuilder result )
        {
            // FIXME: not really correct
            result.append( defaultExclusion );
            result.append( "*" );
            for ( Map.Entry<String, Exclusion> entry : exclusion.entrySet() )
            {
                result.append( "," );
                result.append( entry.getValue() );
                result.append( entry.getKey() );
            }
        }

        @Override
        Iterator<Relationship> doExpand( final Node start )
        {
            return new FilteringIterator<Relationship>(
                    start.getRelationships().iterator(),
                    new Predicate<Relationship>()
                    {
                        public boolean accept( Relationship rel )
                        {
                            Exclusion exclude = exclusion.get( rel.getType().name() );
                            exclude = ( exclude == null ) ? defaultExclusion
                                    : exclude;
                            return exclude.accept( start, rel );
                        }
                    } );
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            Exclusion excluded = exclusion.get( type.name() );
            final Map<String, Exclusion> newExclusion;
            if ( ( ( excluded == null ) ? defaultExclusion : excluded ).includes( direction ) )
            {
                return this;
            }
            else
            {
                excluded = Exclusion.include( direction );
                if ( excluded == defaultExclusion )
                {
                    if ( exclusion.size() == 1 )
                    {
                        return new AllExpander( defaultExclusion.direction );
                    }
                    else
                    {
                        newExclusion = new HashMap<String, Exclusion>(
                                exclusion );
                        newExclusion.remove( type.name() );
                    }
                }
                else
                {
                    newExclusion = new HashMap<String, Exclusion>( exclusion );
                    newExclusion.put( type.name(), excluded );
                }
            }
            return new ExcludingExpander( defaultExclusion, newExclusion );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            Exclusion excluded = exclusion.get( type.name() );
            if ( excluded == Exclusion.ALL )
            {
                return this;
            }
            Map<String, Exclusion> newExclusion = new HashMap<String, Exclusion>(
                    exclusion );
            newExclusion.put( type.name(), Exclusion.ALL );
            return new ExcludingExpander( defaultExclusion, newExclusion );
        }

        @Override
        public StandardExpander reversed()
        {
            Map<String, Exclusion> newExclusion = new HashMap<String, Exclusion>();
            for ( Map.Entry<String, Exclusion> entry : exclusion.entrySet() )
            {
                newExclusion.put( entry.getKey(), entry.getValue().reversed() );
            }
            return new ExcludingExpander( defaultExclusion.reversed(), newExclusion );
        }
    }

    public static final StandardExpander DEFAULT = new AllExpander(
            Direction.BOTH )
    {
        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            return create( type, direction );
        }
    };

    static class RegularExpander extends StandardExpander
    {
        final Map<Direction, RelationshipType[]> types;

        RegularExpander( Map<Direction, RelationshipType[]> types )
        {
            this.types = types;
        }

        @Override
        void buildString( StringBuilder result )
        {
            result.append( types.toString() );
        }
        
        @Override
        Iterator<Relationship> doExpand( final Node start )
        {
            if ( types.isEmpty() )
            {
                return start.getRelationships().iterator();
            }
            else if ( types.size() == 1 )
            {
                Entry<Direction, RelationshipType[]> entry = types.entrySet().iterator().next();
                return start.getRelationships( entry.getKey(), entry.getValue() ).iterator();
            }
            else
            {
                return new NestingIterator<Relationship, Entry<Direction, RelationshipType[]>>( types.entrySet().iterator())
                {
                    @Override
                    protected Iterator<Relationship> createNestedIterator( Entry<Direction, RelationshipType[]> item )
                    {
                        return start.getRelationships( item.getKey(), item.getValue() ).iterator();
                    }
                };
            }
        }
        
        StandardExpander createNew( Map<Direction, RelationshipType[]> types )
        {
            return new RegularExpander( types );
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMapFrom( types );
            tempMap.get( direction ).add( type );
            return createNew( toTypeMap( tempMap ) );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMapFrom( types );
            for ( Direction direction : Direction.values() )
            {
                tempMap.get( direction ).remove( type );
            }
            return createNew( toTypeMap( tempMap ) );
        }

        @Override
        public StandardExpander reversed()
        {
            Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMapFrom( types );
            Collection<RelationshipType> out = tempMap.get( Direction.OUTGOING );
            Collection<RelationshipType> in = tempMap.get( Direction.INCOMING );
            tempMap.put( Direction.OUTGOING, in );
            tempMap.put( Direction.INCOMING, out );
            return createNew( toTypeMap( tempMap ) );
        }
    }

    private static final class FilteringExpander extends StandardExpander
    {
        private final StandardExpander expander;
        private final Filter[] filters;

        FilteringExpander( StandardExpander expander, Filter... filters )
        {
            this.expander = expander;
            this.filters = filters;
        }

        @Override
        void buildString( StringBuilder result )
        {
            expander.buildString( result );
            result.append( "; filter:" );
            for ( Filter filter : filters )
            {
                result.append( " " );
                result.append( filter );
            }
        }

        @Override
        Iterator<Relationship> doExpand( final Node start )
        {
            return new FilteringIterator<Relationship>(
                    expander.doExpand( start ), new Predicate<Relationship>()
                    {
                        public boolean accept( Relationship item )
                        {
                            for ( Filter filter : filters )
                            {
                                if ( filter.exclude( start, item ) )
                                    return false;
                            }
                            return true;
                        }
                    } );
        }

        @Override
        public StandardExpander addNodeFilter( Predicate<? super Node> filter )
        {
            return new FilteringExpander( expander, append( filters,
                    new NodeFilter( filter ) ) );
        }

        @Override
        public StandardExpander addRelationshipFilter(
                Predicate<? super Relationship> filter )
        {
            return new FilteringExpander( expander, append( filters,
                    new RelationshipFilter( filter ) ) );
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            return new FilteringExpander( expander.add( type, direction ),
                    filters );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            return new FilteringExpander( expander.remove( type ), filters );
        }

        @Override
        public StandardExpander reversed()
        {
            return new FilteringExpander( expander.reversed(), filters );
        }
    }

    private static final class TypeLimitingExpander extends StandardExpander
    {
        private final StandardExpander expander;
        private final Map<String, Direction> exclusion;

        TypeLimitingExpander( StandardExpander expander,
                Map<String, Direction> exclusion )
        {
            this.expander = expander;
            this.exclusion = exclusion;
        }

        @Override
        void buildString( StringBuilder result )
        {
            result.append( "*" );
            for ( Map.Entry<String, Direction> entry : exclusion.entrySet() )
            {
                result.append( ",!" );
                if ( entry.getValue() != Direction.BOTH )
                {
                    result.append( entry.getValue().name() );
                    result.append( ":" );
                }
                result.append( entry.getKey() );
            }
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            Direction excluded = exclusion.get( type.name() );
            final Map<String, Direction> newExclusion;
            if ( excluded == null )
            {
                return this;
            }
            else if ( excluded == direction || direction == Direction.BOTH )
            {
                if ( exclusion.size() == 1 )
                {
                    return expander;
                }
                else
                {
                    newExclusion = new HashMap<String, Direction>( exclusion );
                    newExclusion.remove( type.name() );
                }
            }
            else
            {
                newExclusion = new HashMap<String, Direction>( exclusion );
                newExclusion.put( type.name(), direction.reverse() );
            }
            return new TypeLimitingExpander( expander, newExclusion );
        }

        @Override
        Iterator<Relationship> doExpand( final Node start )
        {
            return new FilteringIterator<Relationship>(
                    start.getRelationships().iterator(),
                    new Predicate<Relationship>()
                    {
                        public boolean accept( Relationship item )
                        {
                            Direction dir = exclusion.get( item.getType().name() );
                            return !matchDirection( dir, start, item );
                        }
                    } );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            Direction excluded = exclusion.get( type.name() );
            if ( excluded == Direction.BOTH )
            {
                return this;
            }
            Map<String, Direction> newExclusion = new HashMap<String, Direction>(
                    exclusion );
            newExclusion.put( type.name(), Direction.BOTH );
            return new TypeLimitingExpander( expander, newExclusion );
        }

        @Override
        public StandardExpander reversed()
        {
            Map<String, Direction> newExclusion = new HashMap<String, Direction>();
            for ( Map.Entry<String, Direction> entry : exclusion.entrySet() )
            {
                newExclusion.put( entry.getKey(), entry.getValue().reverse() );
            }
            return new TypeLimitingExpander( expander, newExclusion );
        }
    }

    private static final class WrappingExpander extends StandardExpander
    {
        private static final String IMMUTABLE = "Immutable Expander ";
        private final RelationshipExpander expander;

        WrappingExpander( RelationshipExpander expander )
        {
            this.expander = expander;
        }

        @Override
        void buildString( StringBuilder result )
        {
            result.append( expander );
        }

        @Override
        Iterator<Relationship> doExpand( Node start )
        {
            return expander.expand( start ).iterator();
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            throw new UnsupportedOperationException( IMMUTABLE + expander );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            throw new UnsupportedOperationException( IMMUTABLE + expander );
        }

        @Override
        public StandardExpander reversed()
        {
            throw new UnsupportedOperationException( IMMUTABLE + expander );
        }
    }

    private static abstract class Filter
    {
        abstract boolean exclude( Node start, Relationship item );
    }

    private static final class NodeFilter extends Filter
    {
        private final Predicate<? super Node> predicate;

        NodeFilter( Predicate<? super Node> predicate )
        {
            this.predicate = predicate;
        }

        @Override
        public String toString()
        {
            return predicate.toString();
        }

        @Override
        boolean exclude( Node start, Relationship item )
        {
            return !predicate.accept( item.getOtherNode( start ) );
        }
    }

    private static final class RelationshipFilter extends Filter
    {
        private final Predicate<? super Relationship> predicate;

        RelationshipFilter( Predicate<? super Relationship> predicate )
        {
            this.predicate = predicate;
        }

        @Override
        public String toString()
        {
            return predicate.toString();
        }

        @Override
        boolean exclude( Node start, Relationship item )
        {
            return !predicate.accept( item );
        }
    }

    public final Expansion<Relationship> expand( Node start )
    {
        return new RelationshipExpansion( this, start );
    }

    static <T> T[] append( T[] array, T item )
    {
        @SuppressWarnings( "unchecked" ) T[] result = (T[]) Array.newInstance(
                array.getClass().getComponentType(), array.length + 1 );
        System.arraycopy( array, 0, result, 0, array.length );
        result[array.length] = item;
        return result;
    }

    private static <T> T[] extract( Class<T[]> type, T obj1, T obj2,
            Object[] more, boolean odd )
    {
        if ( more.length % 2 != 0 )
        {
            throw new IllegalArgumentException();
        }
        Object[] target = (Object[]) Array.newInstance(
                type.getComponentType(), ( more.length / 2 ) + 2 );
        try
        {
            target[0] = obj1;
            target[1] = obj2;
            for ( int i = 2; i < target.length; i++ )
            {
                target[i] = more[( i - 2 ) * 2 + ( odd ? 1 : 0 )];
            }
        }
        catch ( ArrayStoreException cast )
        {
            throw new IllegalArgumentException( cast );
        }
        return type.cast( target );
    }

    static boolean matchDirection( Direction dir, Node start, Relationship rel )
    {
        switch ( dir )
        {
        case INCOMING:
            return rel.getEndNode().equals( start );
        case OUTGOING:
            return rel.getStartNode().equals( start );
        }
        return true;
    }

    abstract Iterator<Relationship> doExpand( Node start );

    @Override
    public final String toString()
    {
        StringBuilder result = new StringBuilder( "Expander[" );
        buildString( result );
        result.append( "]" );
        return result.toString();
    }

    abstract void buildString( StringBuilder result );

    public final StandardExpander add( RelationshipType type )
    {
        return add( type, Direction.BOTH );
    }

    public abstract StandardExpander add( RelationshipType type,
            Direction direction );

    public abstract StandardExpander remove( RelationshipType type );

    public abstract StandardExpander reversed();

    public StandardExpander addNodeFilter( Predicate<? super Node> filter )
    {
        return new FilteringExpander( this, new NodeFilter( filter ) );
    }

    public StandardExpander addRelationshipFilter(
            Predicate<? super Relationship> filter )
    {
        return new FilteringExpander( this, new RelationshipFilter( filter ) );
    }
    
    public StandardExpander addRelationsipFilter(
            Predicate<? super Relationship> filter )
    {
        return addRelationshipFilter(filter);
    }

    static StandardExpander wrap( RelationshipExpander expander )
    {
        return new WrappingExpander( expander );
    }

    static Expander create( Direction direction )
    {
        return new AllExpander( direction );
    }

    static StandardExpander create( RelationshipType type, Direction dir )
    {
        Map<Direction, RelationshipType[]> types =
            new EnumMap<Direction, RelationshipType[]>( Direction.class );
        types.put( dir, new RelationshipType[] {type} );
        return new RegularExpander( types );
    }

    static StandardExpander create( RelationshipType type1, Direction dir1,
            RelationshipType type2, Direction dir2 )
    {
        Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMap();
        tempMap.get( dir1 ).add( type1 );
        tempMap.get( dir2 ).add( type2 );
        return new RegularExpander( toTypeMap( tempMap ) );
    }
    
    private static Map<Direction, RelationshipType[]> toTypeMap(
            Map<Direction, Collection<RelationshipType>> tempMap )
    {
        // Remove OUT/IN where there is a BOTH
        Collection<RelationshipType> both = tempMap.get( Direction.BOTH );
        tempMap.get( Direction.OUTGOING ).removeAll( both );
        tempMap.get( Direction.INCOMING ).removeAll( both );
        
        // Convert into a final map
        Map<Direction, RelationshipType[]> map = new EnumMap<Direction, RelationshipType[]>( Direction.class );
        for ( Map.Entry<Direction, Collection<RelationshipType>> entry : tempMap.entrySet() )
        {
            if ( !entry.getValue().isEmpty() )
            {
                map.put( entry.getKey(), entry.getValue().toArray( new RelationshipType[entry.getValue().size()] ) );
            }
        }
        return map;
    }

    private static Map<Direction, Collection<RelationshipType>> temporaryTypeMap()
    {
        Map<Direction, Collection<RelationshipType>> map = new EnumMap<Direction, Collection<RelationshipType>>( Direction.class );
        for ( Direction direction : Direction.values() )
        {
            map.put( direction, new ArrayList<RelationshipType>() );
        }
        return map;
    }

    private static Map<Direction, Collection<RelationshipType>> temporaryTypeMapFrom( Map<Direction, RelationshipType[]> typeMap )
    {
        Map<Direction, Collection<RelationshipType>> map = new EnumMap<Direction, Collection<RelationshipType>>( Direction.class );
        for ( Direction direction : Direction.values() )
        {
            ArrayList<RelationshipType> types = new ArrayList<RelationshipType>();
            map.put( direction, types );
            RelationshipType[] existing = typeMap.get( direction );
            if ( existing != null )
            {
                types.addAll( asList( existing ) );
            }
        }
        return map;
    }
    
    static StandardExpander create( RelationshipType type1, Direction dir1,
            RelationshipType type2, Direction dir2, Object... more )
    {
        Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMap();
        tempMap.get( dir1 ).add( type1 );
        tempMap.get( dir2 ).add( type2 );
        for ( int i = 0; i < more.length; i++ )
        {
            RelationshipType type = (RelationshipType) more[i++];
            Direction direction = (Direction) more[i];
            tempMap.get( direction ).add( type );
        }
        return new RegularExpander( toTypeMap( tempMap ) );
    }
}
