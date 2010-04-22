package org.neo4j.graphdb.traversal;

import org.neo4j.commons.iterator.PrefetchingIterator;

abstract class Traversal extends PrefetchingIterator<Position>
{
    final UniquenessFilter uniquness;
    private final PruneEvaluator pruning;
    private final ReturnFilter filter;

    Traversal( TraversalDescription description )
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
    }

    boolean okToProceed( ExpansionSource source )
    {
        return this.uniquness.check( source, true );
    }
    
    boolean shouldExpandBeyond( ExpansionSource source )
    {
        return !this.pruning.pruneAfter( source.position() ) &&
                this.uniquness.check( source, false );
    }

    boolean okToReturn( ExpansionSource source )
    {
        return filter.shouldReturn( source.position() );
    }
}
