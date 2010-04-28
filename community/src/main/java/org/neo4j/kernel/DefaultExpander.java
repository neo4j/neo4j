package org.neo4j.kernel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.commons.iterator.FilteringIterable;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;

/**
 * A convenience for specifying multiple {@link RelationshipType} /
 * {@link Direction} pairs.
 */
public class DefaultExpander implements RelationshipExpander
{
    static RelationshipExpander ALL = new DefaultExpander(
            new RelationshipType[0], new HashMap<String, Direction>() );
    
    private final RelationshipType[] types;
    private final Map<String, Direction> directions;
    
    protected DefaultExpander( RelationshipType[] types,
            Map<String, Direction> directions)
    {
        this.types = types;
        this.directions = directions;
    }
    
    public DefaultExpander()
    {
        this.types = new RelationshipType[0];
        this.directions = new HashMap<String, Direction>();
    }
    
    protected RelationshipType[] getTypes()
    {
        return this.types;
    }
    
    protected Direction getDirection( RelationshipType type )
    {
        return this.directions.get( type );
    }

    /* (non-Javadoc)
     * @see org.neo4j.kernel.impl.core.RelationshipExp#expand(org.neo4j.graphdb.Node)
     */
    public Iterable<Relationship> expand( final Node start )
    {
        return expand( start, false );
    }
    
    /* (non-Javadoc)
     * @see org.neo4j.kernel.impl.core.RelationshipExp#expand(org.neo4j.graphdb.Node, boolean)
     */
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
        return getRelationshipsForMultipleTypes( start, reversedDirection,
                types, directions );
    }
    
    protected Iterable<Relationship> getRelationshipsForMultipleTypes(
            final Node start, final boolean reversedDirection,
            RelationshipType[] types, final Map<String, Direction> directions )
    {
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
        if ( obj instanceof DefaultExpander )
        {
            DefaultExpander that = (DefaultExpander) obj;
            return Arrays.equals( this.types, that.types ) &&
                    this.directions.equals( that.directions );
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.neo4j.kernel.impl.core.RelationshipExp#add(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)
     */
    public DefaultExpander add( RelationshipType type, Direction direction )
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
        return (DefaultExpander) newExpander(newTypes, newDirections);
    }
    
    protected RelationshipExpander newExpander( RelationshipType[] types,
            Map<String, Direction> directions )
    {
        return new DefaultExpander( types, directions );
    }
}
