package org.neo4j.kernel;

import java.util.Arrays;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.traversal.TraversalDescriptionImpl;

/**
 * A factory for objects regarding traversal of the graph. F.ex. it has a
 * method {@link #createTraversalDescription()} for creating a new
 * {@link TraversalDescription}, methods for creating new
 * {@link ExpansionSource} instances and more.
 */
public class TraversalFactory
{
    /**
     * Creates a new {@link TraversalDescription} with default value for
     * everything so that it's OK to call
     * {@link TraversalDescription#traverse(org.neo4j.graphdb.Node)} without
     * modification. But it isn't a very useful traversal, instead you should
     * add rules and behaviours to it before traversing.
     * 
     * @return a new {@link TraversalDescription} with default values.
     */
    public static TraversalDescription createTraversalDescription()
    {
        return new TraversalDescriptionImpl();
    }
    
    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with {@code type} and {@code direction}.
     * 
     * @param type the {@link RelationshipType} to expand.
     * @param dir the {@link Direction} to expand.
     * @return a new {@link RelationshipExpander}.
     */
    public static RelationshipExpander expanderForTypes( RelationshipType type,
            Direction dir )
    {
        return new DefaultExpander().add( type, dir );
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with two different types and directions.
     * 
     * @param type1 a {@link RelationshipType} to expand.
     * @param dir1 a {@link Direction} to expand.
     * @param type2 another {@link RelationshipType} to expand.
     * @param dir2 another {@link Direction} to expand.
     * @return a new {@link RelationshipExpander}.
     */
    public static RelationshipExpander expanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2 )
    {
        return new DefaultExpander().add( type1, dir1 ).add( type2, dir2 );
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with multiple types and directions.
     * 
     * @param type1 a {@link RelationshipType} to expand.
     * @param dir1 a {@link Direction} to expand.
     * @param type2 another {@link RelationshipType} to expand.
     * @param dir2 another {@link Direction} to expand.
     * @param more additional pairs or type/direction to expand.
     * @return a new {@link RelationshipExpander}.
     */
    public static RelationshipExpander expanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        RelationshipType[] types = extract(
                RelationshipType[].class, type1, type2, more, false );
        Direction[] directions = extract( Direction[].class, dir1, dir2, more, true );
        DefaultExpander expander = new DefaultExpander();
        for ( int i = 0; i < types.length; i++ )
        {
            expander = expander.add( types[i], directions[i] );
        }
        return expander;
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
    
    /**
     * A {@link PruneEvaluator} which prunes everything beyond {@code depth}.
     * @param depth the depth to prune beyond (after).
     * @return a {@link PruneEvaluator} which prunes everything after
     * {@code depth}.
     */
    public static PruneEvaluator pruneAfterDepth( final int depth )
    {
        return new PruneEvaluator()
        {
            public boolean pruneAfter( Position position )
            {
                return position.depth() >= depth;
            }
        };
    }
}
