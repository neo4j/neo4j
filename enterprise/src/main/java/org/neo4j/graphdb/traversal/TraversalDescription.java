package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;

public final class TraversalDescription
{
    public TraversalDescription()
    {
        this( RelationshipExpander.ALL, Uniqueness.NODE_GLOBAL, null,
                PruneEvaluator.NONE, ReturnFilter.ALL, Order.DEPTH_FIRST );
    }

    final RelationshipExpander expander;
    final Uniqueness uniqueness;
    final Object uniquenessParameter;
    PruneEvaluator pruning;
    final ReturnFilter filter;
    final Order order;

    private TraversalDescription( RelationshipExpander expander,
            Uniqueness uniqueness, Object uniquenessParameter,
            PruneEvaluator pruning, ReturnFilter filter, Order order )
    {
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.pruning = pruning;
        this.filter = filter;
        this.order = order;
    }

    public Traverser traverse( Node startNode )
    {
        return new TraverserImpl( this, startNode );
    }

    public TraversalDescription uniqueness( Uniqueness uniqueness )
    {
        return new TraversalDescription( expander, uniqueness, null, pruning,
                filter, order );
    }

    public TraversalDescription uniqueness( Uniqueness uniqueness,
            Object parameter )
    {
        if ( this.uniqueness == uniqueness )
        {
            return this;
        }
        
        switch ( uniqueness )
        {
        case NODE_RECENT:
        case RELATIONSHIP_RECENT:
            acceptIntegerNumber( uniqueness, parameter );
            break;
            
        default:
            throw new IllegalArgumentException(
                    uniqueness.name() + " doesn't accept any parameters" );
        }
        
        return new TraversalDescription( expander, uniqueness, parameter,
                pruning, filter, order );
    }

    private static void acceptIntegerNumber( Uniqueness uniqueness,
            Object parameter )
    {
        boolean isDecimalNumber = parameter instanceof Number
                                  && !( parameter instanceof Float || parameter instanceof Double );
        if ( !isDecimalNumber )
        {
            throw new IllegalArgumentException(
                    uniqueness + " doesn't accept non-decimal values"
                            + ", like '" + parameter + "'" );
        }
    }

    /**
     * Adds {@code pruning} to the list of {@link PruneEvaluator}s which
     * are used to prune the traversal. If any one of the added
     * prune evaluators returns {@code true} it is considerer {@code true}.
     * @param pruning
     * @return
     */
    public TraversalDescription prune( PruneEvaluator pruning )
    {
        if ( this.pruning == pruning )
        {
            return this;
        }
        
        nullCheck( pruning, PruneEvaluator.class, "NO_PRUNING" );
        return new TraversalDescription( expander, uniqueness,
                uniquenessParameter, addPruneEvaluator( pruning ),
                filter, order );
    }
    
    private PruneEvaluator addPruneEvaluator( PruneEvaluator pruning )
    {
        if ( this.pruning instanceof MultiPruneEvaluator )
        {
            return ((MultiPruneEvaluator) this.pruning).add( pruning );
        }
        else
        {
            if ( this.pruning == PruneEvaluator.NONE )
            {
                return pruning;
            }
            else
            {
                return new MultiPruneEvaluator( new PruneEvaluator[] {
                        this.pruning, pruning } );
            }
        }
    }

    public TraversalDescription filter( ReturnFilter filter )
    {
        if ( this.filter == filter )
        {
            return this;
        }
        
        nullCheck( filter, ReturnFilter.class, "ALL" );
        return new TraversalDescription( expander, uniqueness,
                uniquenessParameter, pruning, filter, order );
    }

    private static <T> void nullCheck( T parameter, Class<T> parameterType,
            String defaultName )
    {
        if ( parameter == null )
        {
            String typeName = parameterType.getSimpleName();
            throw new IllegalArgumentException( typeName
                                                + " may not be null, use "
                                                + typeName + "." + defaultName
                                                + " instead." );
        }
    }

    public TraversalDescription depthFirst()
    {
        return order( Order.DEPTH_FIRST );
    }
    
    public TraversalDescription breadthFirst()
    {
        return order( Order.BREADTH_FIRST );
    }
    
    private TraversalDescription order( Order order )
    {
        if ( this.order == order )
        {
            return this;
        }
        return new TraversalDescription( expander, uniqueness,
                uniquenessParameter, pruning, filter, order );
    }

    public TraversalDescription relationships( RelationshipType type )
    {
        return relationships( type, Direction.BOTH );
    }

    public TraversalDescription relationships( RelationshipType type,
            Direction direction )
    {
        return expand( expander.add( type, direction ) );
    }
    
    public TraversalDescription expand(RelationshipExpander expander)
    {
        if ( expander.equals( this.expander ) )
        {
            return this;
        }
        return new TraversalDescription( expander,
                uniqueness, uniquenessParameter, pruning, filter, order );
    }
}
