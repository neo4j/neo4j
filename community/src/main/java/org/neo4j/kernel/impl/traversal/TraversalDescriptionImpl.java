package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.SourceSelectorFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.TraversalFactory;

public final class TraversalDescriptionImpl implements TraversalDescription
{
    public TraversalDescriptionImpl()
    {
        this( StandardExpander.DEFAULT, Uniqueness.NODE_GLOBAL, null,
                PruneEvaluator.NONE, ReturnFilter.ALL,
                TraversalFactory.preorderDepthFirstSelector() );
    }

    final Expander expander;
    final Uniqueness uniqueness;
    final Object uniquenessParameter;
    final PruneEvaluator pruning;
    final ReturnFilter filter;
    final SourceSelectorFactory sourceSelector;

    private TraversalDescriptionImpl( Expander expander,
            Uniqueness uniqueness, Object uniquenessParameter,
            PruneEvaluator pruning, ReturnFilter filter,
            SourceSelectorFactory sourceSelector )
    {
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.pruning = pruning;
        this.filter = filter;
        this.sourceSelector = sourceSelector;
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#traverse(org.neo4j.graphdb.Node)
     */
    public Traverser traverse( Node startNode )
    {
        return new TraverserImpl( this, startNode );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness)
     */
    public TraversalDescription uniqueness( Uniqueness uniqueness )
    {
        return new TraversalDescriptionImpl( expander, uniqueness, null, pruning,
                filter, sourceSelector );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness, java.lang.Object)
     */
    public TraversalDescription uniqueness( Uniqueness uniqueness,
            Object parameter )
    {
        if ( this.uniqueness == uniqueness )
        {
            if ( uniquenessParameter == null ? parameter == null
                    : uniquenessParameter.equals( parameter ) )
            {
                return this;
            }
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

        return new TraversalDescriptionImpl( expander, uniqueness, parameter,
                pruning, filter, sourceSelector );
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

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#prune(org.neo4j.graphdb.traversal.PruneEvaluator)
     */
    public TraversalDescription prune( PruneEvaluator pruning )
    {
        if ( this.pruning == pruning )
        {
            return this;
        }

        nullCheck( pruning, PruneEvaluator.class, "NO_PRUNING" );
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, addPruneEvaluator( pruning ),
                filter, sourceSelector );
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

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#filter(org.neo4j.graphdb.traversal.ReturnFilter)
     */
    public TraversalDescription filter( ReturnFilter filter )
    {
        if ( this.filter == filter )
        {
            return this;
        }

        nullCheck( filter, ReturnFilter.class, "ALL" );
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, pruning, filter, sourceSelector );
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

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#order(org.neo4j.graphdb.traversal.Order)
     */
    public TraversalDescription sourceSelector( SourceSelectorFactory selector )
    {
        if ( this.sourceSelector == selector )
        {
            return this;
        }
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, pruning, filter, selector );
    }

    public TraversalDescription depthFirst()
    {
        return sourceSelector( TraversalFactory.preorderDepthFirstSelector() );
    }

    public TraversalDescription breadthFirst()
    {
        return sourceSelector( TraversalFactory.preorderBreadthFirstSelector() );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#relationships(org.neo4j.graphdb.RelationshipType)
     */
    public TraversalDescription relationships( RelationshipType type )
    {
        return relationships( type, Direction.BOTH );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#relationships(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)
     */
    public TraversalDescription relationships( RelationshipType type,
            Direction direction )
    {
        return expand( expander.add( type, direction ) );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#expand(org.neo4j.graphdb.RelationshipExpander)
     */
    public TraversalDescription expand(RelationshipExpander expander)
    {
        if ( expander.equals( this.expander ) )
        {
            return this;
        }
        return new TraversalDescriptionImpl(
                TraversalFactory.expander( expander ), uniqueness,
                uniquenessParameter, pruning, filter, sourceSelector );
    }
}
