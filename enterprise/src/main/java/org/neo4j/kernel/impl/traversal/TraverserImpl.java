package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.Traverser;

class TraverserImpl implements Traverser
{
    final UniquenessFilter uniquness;
    private final PruneEvaluator pruning;
    private final ReturnFilter filter;
    private final SourceSelector sourceSelector;
    private final TraversalDescriptionImpl description;
    private final Node startNode;

    TraverserImpl( TraversalDescriptionImpl description, Node startNode )
    {
        this.description = description;
        this.startNode = startNode;
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

    public Iterator<Position> iterator()
    {
        return new TraverserIterator();
    }

    public Iterable<Node> nodes()
    {
        return new IterableWrapper<Node, Position>( this )
        {
            @Override
            protected Node underlyingObjectToObject( Position position )
            {
                return position.node();
            }
        };
    }

    public Iterable<Relationship> relationships()
    {
        return new IterableWrapper<Relationship, Position>( this )
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                Iterator<Relationship> iter = super.iterator();
                iter.next(); // Skip the first, it is null
                return iter;
            }

            @Override
            protected Relationship underlyingObjectToObject( Position position )
            {
                return position.lastRelationship();
            }
        };
    }

    public Iterable<Path> paths()
    {
        return new IterableWrapper<Path, Position>( this )
        {
            @Override
            protected Path underlyingObjectToObject( Position position )
            {
                return position.path();
            }
        };
    }
    
    boolean okToProceed( ExpansionSource source )
    {
        return this.uniquness.check( source, true );
    }
    
    boolean shouldExpandBeyond( ExpansionSource source )
    {
        return this.uniquness.check( source, false ) &&
                !this.pruning.pruneAfter( source.position() );
    }

    boolean okToReturn( ExpansionSource source )
    {
        return filter.shouldReturn( source.position() );
    }
    
    private class TraverserIterator extends PrefetchingIterator<Position>
    {
        @Override
        protected Position fetchNextOrNull()
        {
            ExpansionSource result = null;
            while ( true )
            {
                result = sourceSelector.nextPosition();
                if ( result == null )
                {
                    break;
                }
                if ( okToReturn( result ) )
                {
                    break;
                }
            }
            return result != null ? result.position() : null;
        }
    }
}
