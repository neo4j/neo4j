package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.SourceSelector;
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
    
    class TraverserIterator extends PrefetchingIterator<Position>
    {
        final UniquenessFilter uniquness;
        private final SourceSelector sourceSelector;
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
            this.sourceSelector = description.sourceSelector.create(
                    new StartNodeExpansionSource( this, startNode,
                            description.expander ) );
        }
        
        boolean okToProceed( ExpansionSource source )
        {
            return this.uniquness.check( source, true );
        }
        
        boolean shouldExpandBeyond( ExpansionSource source )
        {
            return this.uniquness.check( source, false ) &&
                    !description.pruning.pruneAfter( source.position() );
        }

        boolean okToReturn( ExpansionSource source )
        {
            return description.filter.accept( source.position() );
        }
        
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
