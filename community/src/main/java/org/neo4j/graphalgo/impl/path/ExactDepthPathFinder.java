package org.neo4j.graphalgo.impl.path;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.LiteDepthFirstSelector;
import org.neo4j.graphalgo.impl.util.PathImpl.Builder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.SourceSelectorFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.TraversalFactory;

/**
 * Tries to find paths in a graph from a start node to an end node where the
 * length of found paths must be of a certain length. It also detects
 * "super nodes", i.e. nodes which have many relationships and only iterates
 * over such super nodes' relationships up to a supplied threshold. When that
 * threshold is reached such nodes are considered super nodes and are put on a
 * queue for later traversal. This makes it possible to find paths w/o having to
 * traverse heavy super nodes.
 *
 * @author Mattias Persson
 * @author Tobias Ivarsson
 */
public class ExactDepthPathFinder implements PathFinder<Path>
{
    private final RelationshipExpander expander;
    private final int onDepth;
    private final int startThreshold;

    public ExactDepthPathFinder( RelationshipExpander expander, int onDepth,
            int startThreshold )
    {
        this.expander = expander;
        this.onDepth = onDepth;
        this.startThreshold = startThreshold;
    }

    public Iterable<Path> findAllPaths( final Node start, final Node end )
    {
        return new Iterable<Path>()
        {
            public Iterator<Path> iterator()
            {
                return paths( start, end );
            }
        };
    }

    public Path findSinglePath( Node start, Node end )
    {
        Iterator<Path> paths = paths( start, end );
        return paths.hasNext() ? paths.next() : null;
    }

    private Iterator<Path> paths( final Node start, final Node end )
    {
        TraversalDescription base = TraversalFactory.createTraversalDescription().uniqueness(
                Uniqueness.RELATIONSHIP_PATH ).sourceSelector(
                new SourceSelectorFactory()
                {
                    public SourceSelector create( ExpansionSource startSource )
                    {
                        return new LiteDepthFirstSelector( startSource,
                                startThreshold );
                    }
                } );
        final int firstHalf = onDepth / 2;
        Traverser startTraverser = base.prune(
                TraversalFactory.pruneAfterDepth( firstHalf ) ).expand(
                expander ).filter( new Predicate<Position>()
        {
            public boolean accept( Position item )
            {
                return item.depth() == firstHalf;
            }
        } ).traverse( start );
        final int secondHalf = onDepth - firstHalf;
        Traverser endTraverser = base.prune(
                TraversalFactory.pruneAfterDepth( secondHalf ) ).expand(
                expander.reversed() ).filter( new Predicate<Position>()
        {
            public boolean accept( Position item )
            {
                return item.depth() == secondHalf;
            }
        } ).traverse( end );

        final Iterator<Position> startIterator = startTraverser.iterator();
        final Iterator<Position> endIterator = endTraverser.iterator();

        final Map<Node, Visit> visits = new HashMap<Node, Visit>();
        return new PrefetchingIterator<Path>()
        {
            @Override
            protected Path fetchNextOrNull()
            {
                Position[] found = null;
                while ( found == null
                        && ( startIterator.hasNext() || endIterator.hasNext() ) )
                {
                    found = goOneStep( start, startIterator, visits );
                    if ( found == null )
                    {
                        found = goOneStep( end, endIterator, visits );
                    }
                }
                return found != null ? toPath( found, start ) : null;
            }
        };
    }

    private Path toPath( Position[] found, Node start )
    {
        Path startPath = found[0].path();
        Path endPath = found[1].path();
        if ( !startPath.getStartNode().equals( start ) )
        {
            Path tmpPath = startPath;
            startPath = endPath;
            endPath = tmpPath;
        }
        return toBuilder( startPath ).build( toBuilder( endPath ) );
    }

    private Builder toBuilder( Path path )
    {
        Builder builder = new Builder( path.getStartNode() );
        for ( Relationship rel : path.relationships() )
        {
            builder = builder.push( rel );
        }
        return builder;
    }

    private Position[] goOneStep( Node node, Iterator<Position> visitor,
            Map<Node, Visit> visits )
    {
        if ( !visitor.hasNext() )
        {
            return null;
        }
        Position position = visitor.next();
        Visit visit = visits.get( position.node() );
        if ( visit != null )
        {
            if ( visitor != visit.visitor )
            {
                return new Position[] { visit.position, position };
            }
        }
        else
        {
            visits.put( position.node(), new Visit( position, visitor ) );
        }
        return null;
    }

    private static class Visit
    {
        private final Position position;
        private final Iterator<Position> visitor;

        Visit( Position position, Iterator<Position> visitor )
        {
            this.position = position;
            this.visitor = visitor;
        }
    }
}
