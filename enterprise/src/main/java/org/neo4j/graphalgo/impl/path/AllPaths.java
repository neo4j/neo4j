package org.neo4j.graphalgo.impl.path;

import java.util.Iterator;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class AllPaths implements PathFinder<Path>
{
    private final RelationshipExpander expander;
    private final int maxDepth;

    public AllPaths( int maxDepth, RelationshipExpander expander )
    {
        this.maxDepth = maxDepth;
        this.expander = expander;
    }

    public Iterable<Path> findAllPaths( Node start, final Node end )
    {
        Predicate<Path> filter = new Predicate<Path>()
        {
            public boolean accept( Path pos )
            {
                return pos.endNode().equals( end );
            }
        };

        return Traversal.description().expand( expander ).depthFirst().filter( filter ).prune(
                Traversal.pruneAfterDepth( maxDepth ) ).uniqueness( uniqueness() ).traverse( start );
    }

    protected Uniqueness uniqueness()
    {
        return Uniqueness.RELATIONSHIP_PATH;
    }

    public Path findSinglePath( Node start, Node end )
    {
        Iterator<Path> paths = findAllPaths( start, end ).iterator();
        return paths.hasNext() ? paths.next() : null;
    }
}
