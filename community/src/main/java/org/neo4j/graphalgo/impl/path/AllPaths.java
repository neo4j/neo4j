package org.neo4j.graphalgo.impl.path;

import java.util.Iterator;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.TraversalFactory;

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
        ReturnFilter filter = new ReturnFilter()
        {
            public boolean shouldReturn( Position pos )
            {
                return pos.node().equals( end );
            }
        };
        
        return TraversalFactory.createTraversalDescription().expand(
                expander ).depthFirst().filter( filter ).prune(
                        TraversalFactory.pruneAfterDepth( maxDepth ) ).uniqueness(
                                uniqueness() ).traverse( start ).paths();
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
