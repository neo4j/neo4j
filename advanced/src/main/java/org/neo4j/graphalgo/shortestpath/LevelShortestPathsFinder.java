package org.neo4j.graphalgo.shortestpath;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphalgo.PathImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.TraversalFactory;

public class LevelShortestPathsFinder implements PathFinder
{
    public LevelShortestPathsFinder( int maxlength, RelationshipType type, Direction dir )
    {
        this( maxlength, TraversalFactory.expanderForTypes( type, dir ) );
    }

    public LevelShortestPathsFinder( int maxlength, RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2 )
    {
        this( maxlength, TraversalFactory.expanderForTypes( type1, dir1, type2,
                dir2 ) );
    }

    public LevelShortestPathsFinder( int maxlength, RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        this( maxlength, TraversalFactory.expanderForTypes( type1, dir1, type2,
                dir2, more ) );
    }

    private final int maxlength;
    private final RelationshipExpander expander;

    public LevelShortestPathsFinder( int maxlength, RelationshipExpander expander )
    {
        this.maxlength = maxlength;
        this.expander = expander;
    }
    
    public Collection<Path> findPaths( Node start, Node end )
    {
        if ( start.equals( end ) )
        {
            return Arrays.asList( PathImpl.singular( start ) );
        }
        else
        {
            Map<Node, List<PathImpl.Builder>> startMap, endMap;
            startMap = pathBuilderMap( start );
            endMap = pathBuilderMap( end );
            Collection<Path> result = new LinkedList<Path>();
            for ( int depth = 0; depth < maxlength && result.isEmpty(); depth++ )
            {
                Map<Node, List<PathImpl.Builder>> source, target;
                
                // source will be the smallest, target the biggest
                boolean startMapIsSmallest = startMap.size() < endMap.size();
                source = startMapIsSmallest ? startMap : endMap;
                target = startMapIsSmallest ? endMap : startMap;
                
                // Do one level from the smallest side
                Map<Node, List<PathImpl.Builder>> resultMap =
                    search( start, source, target, result );
                if ( startMapIsSmallest )
                {
                    startMap = resultMap;
                }
                else
                {
                    endMap = resultMap;
                }
            }
            return Collections.unmodifiableCollection( result );
        }
    }
    
    /**
     * This is just a wrapper around {@link #findPaths(Node, Node)}
     * so there's no added performance in this implementation.
     */
    public Path findSinglePath( Node start, Node end )
    {
        Collection<Path> paths = findPaths( start, end );
        return paths.isEmpty() ? null : paths.iterator().next();
    }

    private Map<Node, List<PathImpl.Builder>> search( Node start,
            Map<Node, List<PathImpl.Builder>> source,
            Map<Node, List<PathImpl.Builder>> target, Collection<Path> result )
    {
        Map<Node, List<PathImpl.Builder>> replacement = new HashMap<Node, List<PathImpl.Builder>>();
        for ( Map.Entry<Node, List<PathImpl.Builder>> entry : source.entrySet() )
        {
            Node node = entry.getKey();
            List<PathImpl.Builder> paths = entry.getValue();
            for ( Relationship rel : expander.expand( node ) )
            {
                Node other = rel.getOtherNode( node );
                List<PathImpl.Builder> otherPaths = target.get( other );
                for ( PathImpl.Builder path : paths )
                {
                    path = path.push( rel );
                    if ( otherPaths != null ) // we got a match
                    {
                        for ( PathImpl.Builder otherPath : otherPaths )
                        {
                            if ( path.getStartNode().equals( start ) )
                            {
                                result.add( path.build( otherPath ) );
                            }
                            else
                            {
                                result.add( otherPath.build( path ) );
                            }
                        }
                    }
                    if ( result.isEmpty() ) // only needed if we have no result
                    {
                        List<PathImpl.Builder> newPaths = replacement.get( other );
                        if ( newPaths == null )
                        {
                            newPaths = new LinkedList<PathImpl.Builder>();
                            replacement.put( other, newPaths );
                        }
                        newPaths.add( path );
                    }
                }
            }
        }
        return replacement;
    }

    private static Map<Node, List<PathImpl.Builder>> pathBuilderMap( Node node )
    {
        Map<Node, List<PathImpl.Builder>> result = new HashMap<Node, List<PathImpl.Builder>>();
        result.put( node, new LinkedList<PathImpl.Builder>(
                Arrays.asList( new PathImpl.Builder( node ) ) ) );
        return result;
    }
}
