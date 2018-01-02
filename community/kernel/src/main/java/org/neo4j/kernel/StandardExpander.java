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
package org.neo4j.kernel;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.kernel.impl.util.SingleNodePath;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.ExtendedPath.extend;

public abstract class StandardExpander implements Expander, PathExpander
{
    private StandardExpander()
    {
    }

    static abstract class StandardExpansion<T> implements Expansion<T>
    {
        final StandardExpander expander;
        final Path path;
        final BranchState state;

        StandardExpansion( StandardExpander expander, Path path, BranchState state )
        {
            this.expander = expander;
            this.path = path;
            this.state = state;
        }

        String stringRepresentation( String nodesORrelationships )
        {
            return "Expansion[" + path + ".expand( " + expander + " )."
                    + nodesORrelationships + "()]";
        }

        abstract StandardExpansion<T> createNew( StandardExpander expander );

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

        @Override
        public StandardExpansion<T> filterNodes( org.neo4j.helpers.Predicate<? super Node> filter )
        {
            return createNew( expander.addNodeFilter( filter ) );
        }

        @Override
        public StandardExpansion<T> filterNodes( Predicate<? super Node> filter )
        {
            return createNew( expander.addNodeFilter( filter ) );
        }

        @Override
        public StandardExpansion<T> filterRelationships(
                org.neo4j.helpers.Predicate<? super Relationship> filter )
        {
            return createNew( expander.addRelationshipFilter( filter ) );
        }

        @Override
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
            return !expander.doExpand( path, state ).hasNext();
        }

        public StandardExpansion<Node> nodes()
        {
            return new NodeExpansion( expander, path, state );
        }

        public StandardExpansion<Relationship> relationships()
        {
            return new RelationshipExpansion( expander, path, state );
        }

        public StandardExpansion<Pair<Relationship, Node>> pairs()
        {
            return new PairExpansion( expander, path, state );
        }
    }

    private static final class RelationshipExpansion extends
            StandardExpansion<Relationship>
    {
        RelationshipExpansion( StandardExpander expander, Path path, BranchState state )
        {
            super( expander, path, state );
        }

        @Override
        public String toString()
        {
            return stringRepresentation( "relationships" );
        }

        @Override
        StandardExpansion<Relationship> createNew( StandardExpander expander )
        {
            return new RelationshipExpansion( expander, path, state );
        }

        @Override
        public StandardExpansion<Relationship> relationships()
        {
            return this;
        }

        public Iterator<Relationship> iterator()
        {
            return expander.doExpand( path, state );
        }
    }

    private static final class NodeExpansion extends StandardExpansion<Node>
    {
        NodeExpansion( StandardExpander expander, Path path, BranchState state )
        {
            super( expander, path, state );
        }

        @Override
        public String toString()
        {
            return stringRepresentation( "nodes" );
        }

        @Override
        StandardExpansion<Node> createNew( StandardExpander expander )
        {
            return new NodeExpansion( expander, path, state );
        }

        @Override
        public StandardExpansion<Node> nodes()
        {
            return this;
        }

        public Iterator<Node> iterator()
        {
            final Node node = path.endNode();
            return new IteratorWrapper<Node, Relationship>(
                    expander.doExpand( path, state ) )
            {
                @Override
                protected Node underlyingObjectToObject( Relationship rel )
                {
                    return rel.getOtherNode( node );
                }
            };
        }
    }

    private static final class PairExpansion extends
            StandardExpansion<Pair<Relationship, Node>>
    {
        PairExpansion( StandardExpander expander, Path path, BranchState state )
        {
            super( expander, path, state );
        }

        @Override
        public String toString()
        {
            return stringRepresentation( "pairs" );
        }

        @Override
        StandardExpansion<Pair<Relationship, Node>> createNew( StandardExpander expander )
        {
            return new PairExpansion( expander, path, state );
        }

        @Override
        public StandardExpansion<Pair<Relationship, Node>> pairs()
        {
            return this;
        }

        public Iterator<Pair<Relationship, Node>> iterator()
        {
            final Node node = path.endNode();
            return new IteratorWrapper<Pair<Relationship, Node>, Relationship>(
                    expander.doExpand( path, state ) )
            {
                @Override
                protected Pair<Relationship, Node> underlyingObjectToObject(
                        Relationship rel )
                {
                    return Pair.of( rel, rel.getOtherNode( node ) );
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
        Iterator<Relationship> doExpand( Path path, BranchState state )
        {
            return path.endNode().getRelationships( direction ).iterator();
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
            return new ExcludingExpander( Exclusion.include( direction ), exclude );
        }

        @Override
        public StandardExpander reversed()
        {
            return reverse();
        }

        @Override
        public StandardExpander reverse()
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
        Iterator<Relationship> doExpand( Path path, BranchState state )
        {
            final Node node = path.endNode();
            return new FilteringIterator<Relationship>(
                    node.getRelationships().iterator(),
                    new Predicate<Relationship>()
                    {
                        public boolean test( Relationship rel )
                        {
                            Exclusion exclude = exclusion.get( rel.getType().name() );
                            exclude = (exclude == null) ? defaultExclusion
                                    : exclude;
                            return exclude.accept( node, rel );
                        }
                    } );
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            Exclusion excluded = exclusion.get( type.name() );
            final Map<String, Exclusion> newExclusion;
            if ( (excluded == null ? defaultExclusion : excluded).includes( direction ) )
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
            return reverse();
        }

        @Override
        public StandardExpander reverse()
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

    public static final StandardExpander EMPTY =
            new RegularExpander( Collections.<Direction, RelationshipType[]>emptyMap() );

    private static class DirectionAndTypes
    {
        final Direction direction;
        final RelationshipType[] types;

        DirectionAndTypes( Direction direction, RelationshipType[] types )
        {
            this.direction = direction;
            this.types = types;
        }
    }

    static class RegularExpander extends StandardExpander
    {
        final Map<Direction, RelationshipType[]> typesMap;
        final DirectionAndTypes[] directions;

        RegularExpander( Map<Direction, RelationshipType[]> types )
        {
            this.typesMap = types;
            this.directions = new DirectionAndTypes[types.size()];
            int i = 0;
            for ( Map.Entry<Direction, RelationshipType[]> entry : types.entrySet() )
            {
                this.directions[i++] = new DirectionAndTypes( entry.getKey(), entry.getValue() );
            }
        }

        @Override
        void buildString( StringBuilder result )
        {
            result.append( typesMap.toString() );
        }

        @Override
        Iterator<Relationship> doExpand( Path path, BranchState state )
        {
            final Node node = path.endNode();
            if ( directions.length == 1 )
            {
                DirectionAndTypes direction = directions[0];
                return node.getRelationships( direction.direction, direction.types ).iterator();
            }
            else
            {
                return new NestingIterator<Relationship, DirectionAndTypes>( new ArrayIterator<DirectionAndTypes>(
                        directions ) )
                {
                    @Override
                    protected Iterator<Relationship> createNestedIterator( DirectionAndTypes item )
                    {
                        return node.getRelationships( item.direction, item.types ).iterator();
                    }
                };
            }
        }

        StandardExpander createNew( Map<Direction, RelationshipType[]> types )
        {
            if ( types.isEmpty() )
            {
                return new AllExpander( Direction.BOTH );
            }
            return new RegularExpander( types );
        }

        @Override
        public StandardExpander add( RelationshipType type, Direction direction )
        {
            Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMapFrom( typesMap );
            tempMap.get( direction ).add( type );
            return createNew( toTypeMap( tempMap ) );
        }

        @Override
        public StandardExpander remove( RelationshipType type )
        {
            Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMapFrom( typesMap );
            for ( Direction direction : Direction.values() )
            {
                tempMap.get( direction ).remove( type );
            }
            return createNew( toTypeMap( tempMap ) );
        }

        @Override
        public StandardExpander reversed()
        {
            return reverse();
        }

        @Override
        public StandardExpander reverse()
        {
            Map<Direction, Collection<RelationshipType>> tempMap = temporaryTypeMapFrom( typesMap );
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
        Iterator<Relationship> doExpand( final Path path, BranchState state )
        {
            return new FilteringIterator<Relationship>(
                    expander.doExpand( path, state ), new Predicate<Relationship>()
            {
                public boolean test( Relationship item )
                {
                    Path extendedPath = extend( path, item );
                    for ( Filter filter : filters )
                    {
                        if ( filter.exclude( extendedPath ) )
                        {
                            return false;
                        }
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
            return reverse();
        }

        @Override
        public StandardExpander reverse()
        {
            return new FilteringExpander( expander.reversed(), filters );
        }
    }

    private static final class WrappingExpander extends StandardExpander
    {
        private static final String IMMUTABLE = "Immutable Expander ";
        private final PathExpander expander;

        WrappingExpander( PathExpander expander )
        {
            this.expander = expander;
        }

        @Override
        void buildString( StringBuilder result )
        {
            result.append( expander );
        }

        @Override
        Iterator<Relationship> doExpand( Path path, BranchState state )
        {
            return expander.expand( path, state ).iterator();
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
            return reverse();
        }

        @Override
        public StandardExpander reverse()
        {
            throw new UnsupportedOperationException( IMMUTABLE + expander );
        }
    }

    private static final class WrappingRelationshipExpander extends StandardExpander
    {
        private static final String IMMUTABLE = "Immutable Expander ";
        private final RelationshipExpander expander;

        WrappingRelationshipExpander( RelationshipExpander expander )
        {
            this.expander = expander;
        }

        @Override
        void buildString( StringBuilder result )
        {
            result.append( expander );
        }

        @Override
        Iterator<Relationship> doExpand( Path path, BranchState state )
        {
            return expander.expand( path.endNode() ).iterator();
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
            return reverse();
        }

        @Override
        public StandardExpander reverse()
        {
            throw new UnsupportedOperationException( IMMUTABLE + expander );
        }
    }

    private static abstract class Filter
    {
        abstract boolean exclude( Path path );
    }

    private static final class NodeFilter extends Filter
    {
        private final org.neo4j.function.Predicate<? super Node> predicate;

        NodeFilter( org.neo4j.function.Predicate<? super Node> predicate )
        {
            this.predicate = predicate;
        }

        @Override
        public String toString()
        {
            return predicate.toString();
        }

        @Override
        boolean exclude( Path path )
        {
            return !predicate.test( path.lastRelationship().getOtherNode( path.endNode() ) );
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
        boolean exclude( Path path )
        {
            return !predicate.test( path.lastRelationship() );
        }
    }

    private static final class PathFilter extends Filter
    {
        private final Predicate<? super Path> predicate;

        PathFilter( Predicate<? super Path> predicate )
        {
            this.predicate = predicate;
        }

        @Override
        public String toString()
        {
            return predicate.toString();
        }

        @Override
        boolean exclude( Path path )
        {
            return !predicate.test( path );
        }
    }

    public final Expansion<Relationship> expand( Node node )
    {
        return new RelationshipExpansion( this, new SingleNodePath( node ), BranchState.NO_STATE );
    }

    public final Expansion<Relationship> expand( Path path, BranchState state )
    {
        return new RelationshipExpansion( this, path, state );
    }

    static <T> T[] append( T[] array, T item )
    {
        @SuppressWarnings("unchecked") T[] result = (T[]) Array.newInstance(
                array.getClass().getComponentType(), array.length + 1 );
        System.arraycopy( array, 0, result, 0, array.length );
        result[array.length] = item;
        return result;
    }

    static boolean matchDirection( Direction dir, Node start, Relationship rel )
    {
        switch ( dir )
        {
            case INCOMING:
                return rel.getEndNode().equals( start );
            case OUTGOING:
                return rel.getStartNode().equals( start );
            case BOTH:
                return true;
        }
        return true;
    }

    abstract Iterator<Relationship> doExpand( Path path, BranchState state );

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

    public abstract StandardExpander reverse();

    public abstract StandardExpander reversed();

    @Override
    public StandardExpander addNodeFilter( org.neo4j.helpers.Predicate<? super Node> filter )
    {
        return new FilteringExpander( this, new NodeFilter( org.neo4j.helpers.Predicates.upgrade( filter ) ) );
    }

    @Override
    public StandardExpander addNodeFilter( Predicate<? super Node> filter )
    {
        return new FilteringExpander( this, new NodeFilter( filter ) );
    }

    @Override
    public final Expander addRelationsipFilter( org.neo4j.helpers.Predicate<? super Relationship> filter )
    {
        return addRelationshipFilter( org.neo4j.helpers.Predicates.upgrade( filter ) );
    }

    @Override
    public final Expander addRelationsipFilter( Predicate<? super Relationship> filter )
    {
        return addRelationshipFilter( filter );
    }

    @Override
    public StandardExpander addRelationshipFilter(
            org.neo4j.helpers.Predicate<? super Relationship> filter )
    {
        return new FilteringExpander( this, new RelationshipFilter( org.neo4j.helpers.Predicates.upgrade( filter ) ) );
    }

    @Override
    public StandardExpander addRelationshipFilter(
            Predicate<? super Relationship> filter )
    {
        return new FilteringExpander( this, new RelationshipFilter( filter ) );
    }

    static StandardExpander wrap( RelationshipExpander expander )
    {
        return new WrappingRelationshipExpander( expander );
    }

    static StandardExpander wrap( PathExpander expander )
    {
        return new WrappingExpander( expander );
    }

    public static PathExpander toPathExpander( RelationshipExpander expander )
    {
        return expander instanceof PathExpander ? (PathExpander) expander : wrap( expander );
    }

    public static StandardExpander create( Direction direction )
    {
        return new AllExpander( direction );
    }

    public static StandardExpander create( RelationshipType type, Direction dir )
    {
        Map<Direction, RelationshipType[]> types =
                new EnumMap<Direction, RelationshipType[]>( Direction.class );
        types.put( dir, new RelationshipType[]{type} );
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
        Map<Direction, Collection<RelationshipType>> map = new EnumMap<Direction,
                Collection<RelationshipType>>( Direction.class );
        for ( Direction direction : Direction.values() )
        {
            map.put( direction, new ArrayList<RelationshipType>() );
        }
        return map;
    }

    private static Map<Direction, Collection<RelationshipType>> temporaryTypeMapFrom( Map<Direction,
            RelationshipType[]> typeMap )
    {
        Map<Direction, Collection<RelationshipType>> map = new EnumMap<Direction,
                Collection<RelationshipType>>( Direction.class );
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

    public static StandardExpander create( RelationshipType type1, Direction dir1,
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
