/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
            return createNew( expander.addRelationsipFilter( filter ) );
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
            return new RelationsipExpansion( expander, start );
        }

        public StandardExpansion<Pair<Relationship, Node>> pairs()
        {
            return new PairExpansion( expander, start );
        }
    }

    private static final class RelationsipExpansion extends
            StandardExpansion<Relationship>
    {
        RelationsipExpansion( StandardExpander expander, Node start )
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
            return new RelationsipExpansion( expander, start );
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
            if ( direction == Direction.BOTH )
            {
                return start.getRelationships().iterator();
            }
            else
            {
                return start.getRelationships( direction ).iterator();
            }
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
        final RelationshipType[] types;
        final Map<String, Direction> directions;

        RegularExpander( RelationshipType[] types, Map<String, Direction> dirs )
        {
            this.types = types;
            this.directions = dirs;
        }

        @Override
        void buildString( StringBuilder result )
        {
            String sep = "";
            for ( RelationshipType type : types )
            {
                result.append( sep );
                sep = ",";
                Direction dir = directions.get( type.name() );
                if ( dir != null )
                {
                    result.append( dir.name() );
                    result.append( ":" );
                }
                result.append( type.name() );
            }
        }

        @Override
        Iterator<Relationship> doExpand( final Node start )
        {
            Iterable<Relationship> relationships = start.getRelationships( types );
            if ( directions.isEmpty() )
            {
                return relationships.iterator();
            }
            else
            {
                return new FilteringIterator<Relationship>(
                        relationships.iterator(), new Predicate<Relationship>()
                        {
                            public boolean accept( Relationship rel )
                            {
                                Direction dir = directions.get( rel.getType().name() );
                                return matchDirection(
                                        dir == null ? Direction.BOTH : dir,
                                        start, rel );
                            }
                        } );
            }
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            Map<String, Direction> newDirections = directions;
            if ( direction != Direction.BOTH )
            {
                newDirections = new HashMap<String, Direction>( directions );
                newDirections.put( type.name(), direction );
            }
            return createNew( append( types, type ), newDirections );
        }

        RegularExpander createNew( RelationshipType[] newTypes,
                Map<String, Direction> newDirections )
        {
            return new RegularExpander( newTypes, newDirections );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            for ( int i = 0; i < types.length; i++ )
            {
                if ( type.name().equals( types[i].name() ) )
                {
                    Map<String, Direction> newDirections = directions;
                    if ( directions.containsKey( type.name() ) )
                    {
                        newDirections = new HashMap<String, Direction>(
                                directions );
                    }
                    RelationshipType[] newTypes = new RelationshipType[types.length - 1];
                    System.arraycopy( types, 0, newTypes, 0, i );
                    System.arraycopy( types, i + 1, newTypes, i, types.length
                                                                 - i - 1 );
                    return createNew( types, newDirections );
                }
            }
            return this;
        }

        @Override
        public StandardExpander reversed()
        {
            if ( directions.isEmpty() )
            {
                return this;
            }
            else
            {
                Map<String, Direction> newDirections = new HashMap<String, Direction>();
                for ( Map.Entry<String, Direction> entry : directions.entrySet() )
                {
                    newDirections.put( entry.getKey(),
                            entry.getValue().reverse() );
                }
                return createNew( types, newDirections );
            }
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
        public StandardExpander addRelationsipFilter(
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
        return new RelationsipExpansion( this, start );
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

    public StandardExpander addRelationsipFilter(
            Predicate<? super Relationship> filter )
    {
        return new FilteringExpander( this, new RelationshipFilter( filter ) );
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
        final Map<String, Direction> dirs = new HashMap<String, Direction>();
        if ( dir != Direction.BOTH )
        {
            dirs.put( type.name(), dir );
        }
        return new RegularExpander( new RelationshipType[] { type }, dirs );
    }

    static StandardExpander create( RelationshipType type1, Direction dir1,
            RelationshipType type2, Direction dir2 )
    {
        final Map<String, Direction> dirs = new HashMap<String, Direction>();
        if ( dir1 != Direction.BOTH )
        {
            dirs.put( type1.name(), dir1 );
        }
        if ( dir2 != Direction.BOTH )
        {
            dirs.put( type2.name(), dir2 );
        }
        return new RegularExpander( new RelationshipType[] { type1, type2 },
                dirs );
    }

    static StandardExpander create( RelationshipType type1, Direction dir1,
            RelationshipType type2, Direction dir2, Object... more )
    {
        RelationshipType[] types = extract( RelationshipType[].class, type1,
                type2, more, false );
        Direction[] directions = extract( Direction[].class, dir1, dir2, more,
                true );
        final Map<String, Direction> dirs = new HashMap<String, Direction>();
        for ( int i = 0; i < directions.length; i++ )
        {
            if ( directions[i] != Direction.BOTH )
            {
                dirs.put( types[i].name(), directions[i] );
            }
        }
        return new RegularExpander( types, dirs );
    }
}
