package org.neo4j.kernel;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.commons.iterator.NestingIterable;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;

public class OrderedByTypeExpander extends DefaultExpander
{
    public OrderedByTypeExpander()
    {
    }
    
    protected OrderedByTypeExpander( RelationshipType[] types,
            Map<String, Direction> directions )
    {
        super( types, directions );
    }
    
    @Override
    protected Iterable<Relationship> getRelationshipsForMultipleTypes(
            final Node start, final boolean reversedDirection, RelationshipType[] types,
            final Map<String, Direction> directions )
    {
        return new NestingIterable<Relationship, RelationshipType>( Arrays.asList( types ) )
        {
            @Override
            protected Iterator<Relationship> createNestedIterator(
                    RelationshipType item )
            {
                Direction direction = directions.get( item.name() );
                direction = direction == null ? Direction.BOTH : direction;
                direction = reversedDirection ? direction.reverse() : direction;
                return direction == Direction.BOTH ?
                        start.getRelationships( item ).iterator() :
                            start.getRelationships( item, direction ).iterator();
            }
        };
    }
    
    @Override
    protected RelationshipExpander newExpander( RelationshipType[] types,
            Map<String, Direction> directions )
    {
        return new OrderedByTypeExpander( types, directions );
    }
}
