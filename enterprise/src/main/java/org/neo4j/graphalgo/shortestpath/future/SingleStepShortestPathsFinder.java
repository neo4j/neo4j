package org.neo4j.graphalgo.shortestpath.future;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.commons.iterator.NestingIterator;
import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Find (all or one) simple shortest path(s) between two nodes. It starts
 * from both ends and goes one relationship at the time, alternating side
 * between each traversal. It does so to minimize the traversal overhead
 * if one side has a very large amount of relationships, but the other one
 * very few. It performs well however the graph is proportioned.
 * 
 * TODO At the moment it doesn't return ALL paths, only ALMOST ALL :)
 * but the single shortest path returns correct answers.
 */
public class SingleStepShortestPathsFinder
{
    private final int maxDepth;
    private final RelationshipExpander relExpander;
    
    public SingleStepShortestPathsFinder( int maxDepth, RelationshipExpander relExpander )
    {
        this.maxDepth = maxDepth;
        this.relExpander = relExpander;
    }
    
    public SingleStepShortestPathsFinder( int maxDepth, RelationshipType relType,
            Direction direction )
    {
        this( maxDepth, RelationshipExpander.forTypes( relType, direction ) );
    }
    
    public Collection<Path> paths( Node start, Node end )
    {
        return internalPaths( start, end, false );
    }
    
    public Path path( Node start, Node end )
    {
        Collection<Path> paths = internalPaths( start, end, true );
        return !paths.isEmpty() ? paths.iterator().next() : null;
    }
    
    private Collection<Path> internalPaths( Node start, Node end,
            boolean stopAsap )
    {
        if ( start.equals( end ) )
        {
            return Arrays.asList( Path.singular( start ) );
        }

        Map<Integer, List<Path>> hits =
                new HashMap<Integer, List<Path>>();
        Collection<Long> sharedVisitedRels = new HashSet<Long>();
        FrozenDepth sharedFrozenDepth = new FrozenDepth();
        MutableBoolean sharedStop = new MutableBoolean();
        MutableInteger sharedCurrentDepth = new MutableInteger();
        final DirectionData startData = new DirectionData( "start", start,
                sharedVisitedRels, sharedFrozenDepth, sharedStop,
                sharedCurrentDepth );
        final DirectionData endData = new DirectionData( "end", end,
                sharedVisitedRels, sharedFrozenDepth, sharedStop,
                sharedCurrentDepth );
        
        while ( startData.hasNext() || endData.hasNext() )
        {
            goOneStep( startData, endData, hits, stopAsap, startData );
            goOneStep( endData, startData, hits, stopAsap, startData );
        }
        return least( hits );
    }
    
    private Collection<Path> least( Map<Integer, List<Path>> hits )
    {
        if ( hits.size() == 0 )
        {
            return Collections.emptyList();
        }
        
        // TODO Fix this somehow, this is like... well, it's like something...
        for ( int i = 0; true; i++ )
        {
            List<Path> paths = hits.get( i );
            if ( paths != null )
            {
                return paths;
            }
        }
    }

    private void goOneStep( DirectionData directionData,
            DirectionData otherSide, Map<Integer, List<Path>> hits,
            boolean stopAsEarlyAsPossible, DirectionData startSide )
    {
        if ( !directionData.hasNext() )
        {
            return;
        }
        
        LevelData levelData = directionData.next();
        LevelData otherSideHit = otherSide.visitedNodes.get( levelData.node );
        if ( otherSideHit != null )
        {
            // This is a hit
            int depth = directionData.currentDepth + otherSideHit.depth;
            if ( !directionData.sharedFrozenDepth.isFrozen() )
            {
                directionData.sharedFrozenDepth.depth = depth;
            }
            if ( depth <= directionData.sharedFrozenDepth.depth )
            {
                directionData.haveFoundSomething = true;
                if ( depth < directionData.sharedFrozenDepth.depth )
                {
                    directionData.sharedFrozenDepth.depth = depth;
                    // TODO Is it really ok to just stop the other side here?
                    // I'm basing that decision on that it was the other side
                    // which found the deeper paths (correct assumption?)
                    otherSide.stop = true;
                    if ( stopAsEarlyAsPossible )
                    {
                        // we can stop here because we won't get a more shallow
                        // path than this.
                        directionData.sharedStop.value = true;
                    }
                }
                
                List<Path> paths = hits.get( depth );
                if ( paths == null )
                {
                    paths = new ArrayList<Path>();
                    hits.put( depth, paths );
                }
                
                Path.Builder startPath = directionData == startSide ?
                        levelData.path : otherSideHit.path;
                Path.Builder endPath = directionData == startSide ?
                        otherSideHit.path : levelData.path;
                paths.add( startPath.build( endPath ) );
                
                // TODO Even if stopAsap=true we can't pull the emergency stop
                // here since it could just be that the other side will find
                // paths more shallow than this... 
            }
            if ( depth == 1 )
            {
                directionData.sharedStop.value = true;
            }
        }
    }
    
    private class DirectionData extends PrefetchingIterator<LevelData>
    {
        private final String name;
        private int currentDepth;
        private Iterator<Relationship> nextRelationships;
        private final Collection<Node> nextNodes = new ArrayList<Node>();
        private Map<Node, LevelData> visitedNodes =
                new HashMap<Node, LevelData>();
        private Node lastParentTraverserNode;
        private final Collection<Long> sharedVisitedRels;
        private final FrozenDepth sharedFrozenDepth;
        private final MutableBoolean sharedStop;
        private final MutableInteger sharedCurrentDepth;
        private boolean haveFoundSomething;
        private boolean stop;
//        private final Map<Node, List<Path.Builder>> paths =
//                new HashMap<Node, List<Path.Builder>>();
        
        DirectionData( String name, Node startNode,
                Collection<Long> sharedVisitedRels,
                FrozenDepth sharedFrozenDepth, MutableBoolean sharedStop,
                MutableInteger sharedCurrentDepth )
        {
            this.name = name;
            this.visitedNodes.put( startNode, new LevelData( startNode,
                    currentDepth, new Path.Builder( startNode ) ) );
            this.nextNodes.add( startNode );
            this.sharedVisitedRels = sharedVisitedRels;
            this.sharedFrozenDepth = sharedFrozenDepth;
            this.sharedStop = sharedStop;
            this.sharedCurrentDepth = sharedCurrentDepth;
            prepareNextLevel();
        }
        
        private void prepareNextLevel()
        {
            Collection<Node> nodesToIterate =
                    new ArrayList<Node>( this.nextNodes );
            this.nextNodes.clear();
            this.nextRelationships = new NestingIterator<Relationship, Node>(
                    nodesToIterate.iterator() )
            {
                @Override
                protected Iterator<Relationship> createNestedIterator(
                        Node node )
                {
                    lastParentTraverserNode = node;
                    return relExpander.expand( node ).iterator();
                }
            };
            this.currentDepth++;
            this.sharedCurrentDepth.value++;
        }
        
        @Override
        protected LevelData fetchNextOrNull()
        {
            while ( true ) // TODO limit the loop?
            {
                Relationship nextRel = fetchNextRelOrNull();
                if ( nextRel == null )
                {
                    return null;
                }
                if ( !sharedVisitedRels.add( nextRel.getId() ) )
                {
                    continue;
                }
                
                Node result =
                    nextRel.getOtherNode( this.lastParentTraverserNode );
                if ( this.visitedNodes.containsKey( result ) )
                {
                    // TODO Do something if we've already been here?
                    continue;
                }
                
                Path.Builder parentPath = this.visitedNodes.get(
                        lastParentTraverserNode ).path;
                LevelData levelData = new LevelData( result,
                        currentDepth, parentPath.push( nextRel ) );
                this.visitedNodes.put( result, levelData );
                this.nextNodes.add( result );
                return levelData;
            }
        }
        
        private boolean canGoDeeper()
        {
            return !this.sharedFrozenDepth.isFrozen()
                    && this.sharedCurrentDepth.value < maxDepth;
        }
        
        private Relationship fetchNextRelOrNull()
        {
            boolean stopped = this.stop || this.sharedStop.value;
            boolean hasComeTooFarEmptyHanded = this.sharedFrozenDepth.isFrozen()
                    && sharedCurrentDepth.value > sharedFrozenDepth.depth
                    && !this.haveFoundSomething;
            if ( stopped || hasComeTooFarEmptyHanded )
            {
                return null;
            }
            
            if ( !this.nextRelationships.hasNext() )
            {
                if ( canGoDeeper() )
                {
                    prepareNextLevel();
                }
            }
            return this.nextRelationships.hasNext() ?
                    this.nextRelationships.next() : null;
        }

        @Override public String toString()
        {
            return this.name;
        }
    }
    
    private class MutableBoolean
    {
        private boolean value;
    }
    
    private class MutableInteger
    {
        private int value;
    }
    
    private class LevelData
    {
        private final Node node;
        private final int depth;
        private final Path.Builder path;
        
        LevelData( Node node, int depth, Path.Builder pathToHere )
        {
            this.node = node;
            this.depth = depth;
            this.path = pathToHere;
        }
    }
    
    private class FrozenDepth
    {
        private Integer depth;
        
        boolean isFrozen()
        {
            return this.depth != null;
        }
    }
}
