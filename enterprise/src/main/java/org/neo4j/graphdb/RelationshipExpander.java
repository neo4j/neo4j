package org.neo4j.graphdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.commons.iterator.FilteringIterable;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * A convenience for specifying multiple {@link RelationshipType} /
 * {@link Direction} pairs.
 */
public class RelationshipExpander
{
    public static final RelationshipExpander ALL = new RelationshipExpander(
            new RelationshipType[0], new Direction[0] );

    private final RelationshipType[] types;
    private final Map<String, Direction> directions;
    
    private RelationshipExpander( RelationshipType[] types,
            Map<String, Direction> directions)
    {
        this.types = types;
        this.directions = directions;
    }

    private RelationshipExpander( RelationshipType[] types, Direction[] dirs )
    {
        if ( types.length != dirs.length )
        {
            throw new IllegalArgumentException();
        }
        this.types = new RelationshipType[types.length];
        this.directions = new HashMap<String, Direction>();
        for ( int i = 0; i < types.length; i++ )
        {
            this.types[i] = types[i];
            this.directions.put( types[i].name(), dirs[i] );
        }
    }

    public Iterable<Relationship> expand( final Node start )
    {
        return expand( start, false );
    }
    
    public Iterable<Relationship> expand( final Node start,
            final boolean reversedDirection )
    {
        if ( types.length == 0 )
        {
            return start.getRelationships();
        }
        if ( types.length == 1 )
        {
            RelationshipType type = types[0];
            Direction direction = directions.get( type.name() );
            return start.getRelationships( type,
                    reversedDirection ? direction.reverse() : direction );
        }
        return new FilteringIterable<Relationship>(
                start.getRelationships( types ) )
        {
            @Override
            protected boolean passes( Relationship item )
            {
                switch ( directions.get( item.getType().name() ) )
                {
                case INCOMING:
                    return reversedDirection ? item.getStartNode().equals( start ) :
                            item.getEndNode().equals( start );
                case OUTGOING:
                    return reversedDirection ? item.getEndNode().equals( start ) :
                            item.getStartNode().equals( start );
                default:
                    return true;
                }
            }
        };
    }
    
    @Override
    public int hashCode()
    {
        return Arrays.hashCode( types );
    }

    @Override
    public boolean equals( Object obj )
    {
        if (this == obj)
        {
            return true;
        }
        if ( obj instanceof RelationshipExpander )
        {
            RelationshipExpander that = (RelationshipExpander) obj;
            return Arrays.equals( this.types, that.types ) && this.directions.equals( that.directions );
        }
        return false;
    }

    public RelationshipExpander add( RelationshipType type, Direction direction )
    {
        Direction existingDirection = directions.get( type.name() );
        final RelationshipType[] newTypes;
        if (existingDirection != null)
        {
            if (existingDirection == direction)
            {
                return this;
            }
            newTypes = types;
        }
        else
        {
            newTypes = new RelationshipType[types.length + 1];
            System.arraycopy( types, 0, newTypes, 0, types.length );
            newTypes[types.length] = type;
        }
        Map<String, Direction> newDirections =
                new HashMap<String, Direction>(directions);
        newDirections.put( type.name(), direction );
        return new RelationshipExpander(newTypes, newDirections);
    }

    public static RelationshipExpander forTypes( RelationshipType type,
            Direction dir )
    {
        return new RelationshipExpander( new RelationshipType[] { type },
                new Direction[] { dir } );
    }

    public static RelationshipExpander forTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2 )
    {
        return new RelationshipExpander(
                new RelationshipType[] { type1, type2 }, new Direction[] {
                        dir1, dir2 } );
    }

    public static RelationshipExpander forTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        return new RelationshipExpander( extract( RelationshipType[].class,
                type1, type2, more, false ), extract( Direction[].class, dir1,
                dir2, more, true ) );
    }

    private static Object[] EMPTY_ARRAY = new Object[0];

    private static <T> T[] extract( Class<T[]> type, T obj1, T obj2,
            Object[] more, boolean odd )
    {
        if ( more.length % 2 != 0 )
        {
            throw new IllegalArgumentException();
        }
        Object[] target = Arrays.copyOf( EMPTY_ARRAY, ( more.length / 2 ) + 2,
                type );
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
}
