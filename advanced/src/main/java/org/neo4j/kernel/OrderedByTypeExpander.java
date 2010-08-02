package org.neo4j.kernel;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.NestingIterator;

public final class OrderedByTypeExpander extends
        StandardExpander.RegularExpander
{
    public OrderedByTypeExpander()
    {
        this( new RelationshipType[0],
                Collections.<String, Direction>emptyMap() );
    }

    OrderedByTypeExpander( RelationshipType[] types,
            Map<String, Direction> directions )
    {
        super( types, directions );
    }

    @Override
    RegularExpander createNew( RelationshipType[] newTypes,
            Map<String, Direction> newDirections )
    {
        return new OrderedByTypeExpander( newTypes, newDirections );
    }

    @Override
    Iterator<Relationship> doExpand( final Node start )
    {
        return new NestingIterator<Relationship, RelationshipType>(
                new ArrayIterator<RelationshipType>( types ) )
        {
            @Override
            protected Iterator<Relationship> createNestedIterator(
                    RelationshipType type )
            {
                Direction dir = directions.get( type.name() );
                dir = ( dir == null ) ? Direction.BOTH : dir;
                return ( ( dir == Direction.BOTH ) ? start.getRelationships( type )
                        : start.getRelationships( type, dir ) ).iterator();
            }
        };
    }
}
