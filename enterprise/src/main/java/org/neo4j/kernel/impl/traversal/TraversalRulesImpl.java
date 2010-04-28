package org.neo4j.kernel.impl.traversal;

import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalRules;

class TraversalRulesImpl extends PrefetchingIterator<Position>
        implements TraversalRules
{
    final UniquenessFilter uniquness;
    private final PruneEvaluator pruning;
    private final ReturnFilter filter;
    private final SourceSelector sourceSelector;
    
    TraversalRulesImpl( TraversalDescriptionImpl description,
            Node startNode )
    {
        PrimitiveTypeFetcher type = PrimitiveTypeFetcher.NODE;
        switch ( description.uniqueness )
        {
        case RELATIONSHIP_GLOBAL:
            type = PrimitiveTypeFetcher.RELATIONSHIP;
        case NODE_GLOBAL:
            this.uniquness = new GloballyUnique( type );
            break;
        case RELATIONSHIP_PATH:
            type = PrimitiveTypeFetcher.RELATIONSHIP;
        case NODE_PATH:
            this.uniquness = new PathUnique( type );
            break;
        case RELATIONSHIP_RECENT:
            type = PrimitiveTypeFetcher.RELATIONSHIP;
        case NODE_RECENT:
            this.uniquness = new RecentlyUnique( type,
                    description.uniquenessParameter );
            break;
        case NONE:
            this.uniquness = new NotUnique();
            break;
        default:
            throw new IllegalArgumentException( "Unknown Uniquness "
                                                + description.uniqueness );
        }
        this.pruning = description.pruning;
        this.filter = description.filter;
        this.sourceSelector = description.sourceSelector.create(
                new ExpansionSourceImpl( this, null, startNode,
                        description.expander, null ) );
    }

    @Override
    protected Position fetchNextOrNull()
    {
        ExpansionSource source = this.sourceSelector.nextPosition( this );
        return source != null ? source.position() : null;
    }

    public boolean okToProceed( ExpansionSource source )
    {
        return this.uniquness.check( source, true );
    }
    
    public boolean shouldExpandBeyond( ExpansionSource source )
    {
        return !this.pruning.pruneAfter( source.position() ) &&
                this.uniquness.check( source, false );
    }

    public boolean okToReturn( ExpansionSource source )
    {
        return filter.shouldReturn( source.position() );
    }
}
