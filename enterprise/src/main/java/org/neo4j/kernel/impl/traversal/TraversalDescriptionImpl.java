package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;

public final class TraversalDescriptionImpl implements TraversalDescription
{
    public TraversalDescriptionImpl()
    {
        this( StandardExpander.DEFAULT, Uniqueness.NODE_GLOBAL, null,
                PruneEvaluator.NONE, Traversal.returnAll(),
                Traversal.preorderDepthFirst() );
    }

    final Expander expander;
    final Uniqueness uniqueness;
    final Object uniquenessParameter;
    final PruneEvaluator pruning;
    final Predicate<Path> filter;
    final BranchOrderingPolicy branchSelector;

    private TraversalDescriptionImpl( Expander expander,
            Uniqueness uniqueness, Object uniquenessParameter,
            PruneEvaluator pruning, Predicate<Path> filter,
            BranchOrderingPolicy branchSelector )
    {
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.pruning = pruning;
        this.filter = filter;
        this.branchSelector = branchSelector;
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
                filter, branchSelector );
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
                pruning, filter, branchSelector );
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
                filter, branchSelector );
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

    public TraversalDescription filter( Predicate<Path> filter )
    {
        if ( this.filter == filter )
        {
            return this;
        }

        if ( filter == null )
        {
            throw new IllegalArgumentException( "Return filter may not be null, " +
            		"use " + Traversal.class.getSimpleName() + ".returnAll() instead." );
        }
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, pruning, filter, branchSelector );
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
    public TraversalDescription order( BranchOrderingPolicy selector )
    {
        if ( this.branchSelector == selector )
        {
            return this;
        }
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, pruning, filter, selector );
    }

    public TraversalDescription depthFirst()
    {
        return order( Traversal.preorderDepthFirst() );
    }

    public TraversalDescription breadthFirst()
    {
        return order( Traversal.preorderBreadthFirst() );
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
                Traversal.expander( expander ), uniqueness,
                uniquenessParameter, pruning, filter, branchSelector );
    }
}
