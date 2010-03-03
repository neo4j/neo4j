package org.neo4j.graphalgo.shortestpath.future;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LevelShortestPathsFinder
{
    public LevelShortestPathsFinder( int maxlength, RelationshipType type, Direction dir )
    {
        this( maxlength, RelationshipExpander.forTypes( type, dir ) );
    }

    public LevelShortestPathsFinder( int maxlength, RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2 )
    {
        this( maxlength, RelationshipExpander.forTypes( type1, dir1, type2,
                dir2 ) );
    }

    public LevelShortestPathsFinder( int maxlength, RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        this( maxlength, RelationshipExpander.forTypes( type1, dir1, type2,
                dir2, more ) );
    }

    private final int maxlength;
    private final RelationshipExpander expander;

    public LevelShortestPathsFinder( int maxlength, RelationshipExpander expander )
    {
        this.maxlength = maxlength;
        this.expander = expander;
    }

    public Collection<Path> paths( Node start, Node end )
    {
        if ( start.equals( end ) )
        {
            return Arrays.asList( Path.singular( start ) );
        }
        else
        {
            Map<Node, List<Path.Builder>> source, target, next;
            source = pathBuilderMap( start );
            next = pathBuilderMap( end );
            Collection<Path> result = new LinkedList<Path>();
            for ( int depth = 0; depth < maxlength && result.isEmpty(); depth++ )
            {
                // Search one level from source to target then rotate directions
                // This means that the deepest paths will be from the start
                // node if maxLength is odd. To optimize make sure that the
                // node (start/end) with the least relationships goes deepest
                // instead, it'll most likely be faster that way.
                target = next;
                if ( depth == 2 )
                {
                    // Here we've traversed one level from each node (start/end)
                    // Let's figure out which of those has the least
                    // relationships and see if we can optimize the depth for it
                    if ( source.size() > target.size() )
                    {
                        // Switch 'em
                        Map<Node, List<Path.Builder>> holder = source;
                        source = target;
                        target = holder;
                    }
                }
                next = search( start, source, target, result );
                source = target;
            }
            return Collections.unmodifiableCollection( result );
        }
    }

    private Map<Node, List<Path.Builder>> search( Node start,
            Map<Node, List<Path.Builder>> source,
            Map<Node, List<Path.Builder>> target, Collection<Path> result )
    {
        Map<Node, List<Path.Builder>> replacement = new HashMap<Node, List<Path.Builder>>();
        for ( Map.Entry<Node, List<Path.Builder>> entry : source.entrySet() )
        {
            Node node = entry.getKey();
            List<Path.Builder> paths = entry.getValue();
            for ( Relationship rel : expander.expand( node ) )
            {
                Node other = rel.getOtherNode( node );
                List<Path.Builder> otherPaths = target.get( other );
                for ( Path.Builder path : paths )
                {
                    path = path.push( rel );
                    if ( otherPaths != null ) // we got a match
                    {
                        for ( Path.Builder otherPath : otherPaths )
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
                        List<Path.Builder> newPaths = replacement.get( other );
                        if ( newPaths == null )
                        {
                            newPaths = new LinkedList<Path.Builder>();
                            replacement.put( other, newPaths );
                        }
                        newPaths.add( path );
                    }
                }
            }
        }
        return replacement;
    }

    private static Map<Node, List<Path.Builder>> pathBuilderMap( Node node )
    {
        Map<Node, List<Path.Builder>> result = new HashMap<Node, List<Path.Builder>>();
        result.put( node, new LinkedList<Path.Builder>(
                Arrays.asList( new Path.Builder( node ) ) ) );
        return result;
    }
}
