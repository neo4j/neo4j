package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;

class TraverserImpl implements Traverser
{
    private final TraversalDescriptionImpl description;
    private final Node startNode;

    TraverserImpl( TraversalDescriptionImpl description, Node startNode )
    {
        this.description = description;
        this.startNode = startNode;
    }

    public Iterator<Path> iterator()
    {
        return new TraverserIterator();
    }

    public Iterable<Node> nodes()
    {
        return new IterableWrapper<Node, Path>( this )
        {
            @Override
            protected Node underlyingObjectToObject( Path position )
            {
                return position.endNode();
            }
        };
    }

    public Iterable<Relationship> relationships()
    {
        return new IterableWrapper<Relationship, Path>( this )
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                Iterator<Relationship> iter = super.iterator();
                iter.next(); // Skip the first, it is null
                return iter;
            }

            @Override
            protected Relationship underlyingObjectToObject( Path position )
            {
                return position.lastRelationship();
            }
        };
    }

    class TraverserIterator extends PrefetchingIterator<Path>
    {
        final UniquenessFilter uniquness;
        private final BranchSelector sourceSelector;
        final TraversalDescriptionImpl description;
        final Node startNode;

        TraverserIterator()
        {
            PrimitiveTypeFetcher type = PrimitiveTypeFetcher.NODE;
            this.description = TraverserImpl.this.description;
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
            this.startNode = TraverserImpl.this.startNode;
            this.sourceSelector = description.branchSelector.create(
                    new StartNodeExpansionSource( this, startNode,
                            description.expander ) );
        }

        boolean okToProceed( TraversalBranch source )
        {
            return this.uniquness.check( source, true );
        }

        boolean shouldExpandBeyond( TraversalBranch source )
        {
            return this.uniquness.check( source, false ) &&
                    !description.pruning.pruneAfter( source.position() );
        }

        boolean okToReturn( TraversalBranch source )
        {
            return description.filter.accept( source.position() );
        }

        @Override
        protected Path fetchNextOrNull()
        {
            TraversalBranch result = null;
            while ( true )
            {
                result = sourceSelector.next();
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
