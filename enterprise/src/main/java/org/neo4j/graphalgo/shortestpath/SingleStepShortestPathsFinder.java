package org.neo4j.graphalgo.shortestpath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.commons.iterator.NestingIterator;
import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphalgo.Path;
import org.neo4j.graphalgo.RelationshipExpander;
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

        Map<Integer, Collection<Hit>> hits =
                new HashMap<Integer, Collection<Hit>>();
        Collection<Long> sharedVisitedRels = new HashSet<Long>();
        ValueHolder<Integer> sharedFrozenDepth = new ValueHolder<Integer>( null );
        ValueHolder<Boolean> sharedStop = new ValueHolder<Boolean>( false );
        ValueHolder<Integer> sharedCurrentDepth = new ValueHolder<Integer>( 0 );
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
    
    private Collection<Path> least( Map<Integer, Collection<Hit>> hits )
    {
        if ( hits.size() == 0 )
        {
            return Collections.emptyList();
        }
        
        // FIXME eehhh... loop through from zero, are you kiddin' me?
        for ( int i = 0; true; i++ )
        {
            Collection<Hit> depthHits = hits.get( i );
            if ( depthHits != null )
            {
                return hitsToPaths( depthHits );
            }
        }
    }
    
    private Collection<Path> hitsToPaths( Collection<Hit> depthHits )
    {
        // TODO Do this lazy?
        Collection<Path> paths = new ArrayList<Path>();
        for ( Hit hit : depthHits )
        {
            for ( Path.Builder start : hit.start.paths )
            {
                for ( Path.Builder end : hit.end.paths )
                {
                    paths.add( start.build( end ) );
                }
            }
        }
        return paths;
    }

    private static class Hit
    {
        private final LevelData start;
        private final LevelData end;
        
        Hit( LevelData start, LevelData end )
        {
            this.start = start;
            this.end = end;
        }
        
        @Override
        public int hashCode()
        {
            int result = 17;
            result += start.node.hashCode() * 37;
            result += end.node.hashCode() * 37;
            return result;
        }
        
        @Override
        public boolean equals( Object obj )
        {
            Hit o = (Hit) obj;
            return o.start.node.equals( start.node ) &&
                    o.end.node.equals( end.node );
        }
    }

    private void goOneStep( DirectionData directionData,
            DirectionData otherSide, Map<Integer, Collection<Hit>> hits,
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
            if ( directionData.sharedFrozenDepth.value == null )
            {
                directionData.sharedFrozenDepth.value = depth;
            }
            if ( depth <= directionData.sharedFrozenDepth.value )
            {
                directionData.haveFoundSomething = true;
                if ( depth < directionData.sharedFrozenDepth.value )
                {
                    directionData.sharedFrozenDepth.value = depth;
                    // TODO Is it really ok to just stop the other side here?
                    // I'm basing that decision on that it was the other side
                    // which found the deeper paths (correct assumption?)
                    otherSide.stop = true;
                    if ( stopAsEarlyAsPossible )
                    {
                        // we can stop here because we won't get a less deep path than this.
                        directionData.sharedStop.value = true;
                    }
                }
                
                // Add it to the list of hits
                Collection<Hit> depthHits = hits.get( depth );
                if ( depthHits == null )
                {
                    depthHits = new HashSet<Hit>();
                    hits.put( depth, depthHits );
                }
                LevelData startSideData = directionData == startSide ? levelData : otherSideHit;
                LevelData endSideData = directionData == startSide ? otherSideHit : levelData;
                depthHits.add( new Hit( startSideData, endSideData ) );
            }
        }
    }
    
    private class DirectionData extends PrefetchingIterator<LevelData>
    {
//        private final String name;
        private int currentDepth;
        private Iterator<Relationship> nextRelationships;
        private final Collection<Node> nextNodes = new ArrayList<Node>();
        private Map<Node, LevelData> visitedNodes = new HashMap<Node, LevelData>();
        private Node lastParentTraverserNode;
        private LevelData lastParentLevelData;
//        private final Collection<Long> sharedVisitedRels;
        private final ValueHolder<Integer> sharedFrozenDepth;
        private final ValueHolder<Boolean> sharedStop;
        private final ValueHolder<Integer> sharedCurrentDepth;
        private boolean haveFoundSomething;
        private boolean stop;
        
        DirectionData( String name, Node startNode, Collection<Long> sharedVisitedRels,
                ValueHolder<Integer> sharedFrozenDepth, ValueHolder<Boolean> sharedStop,
                ValueHolder<Integer> sharedCurrentDepth )
        {
//            this.name = name;
            this.visitedNodes.put( startNode, new LevelData( startNode,
                    currentDepth, new Path.Builder( startNode ) ) );
            this.nextNodes.add( startNode );
//            this.sharedVisitedRels = sharedVisitedRels;
            this.sharedFrozenDepth = sharedFrozenDepth;
            this.sharedStop = sharedStop;
            this.sharedCurrentDepth = sharedCurrentDepth;
            prepareNextLevel();
        }
        
//        private void debug( String text )
//        {
//            System.out.println( this.name + ":" + text );
//        }
        
        private void prepareNextLevel()
        {
            Collection<Node> nodesToIterate = new ArrayList<Node>( this.nextNodes );
            this.nextNodes.clear();
            this.nextRelationships = new NestingIterator<Relationship, Node>(
                    nodesToIterate.iterator() )
            {
                @Override
                protected Iterator<Relationship> createNestedIterator( Node node )
                {
                    lastParentTraverserNode = node;
                    lastParentLevelData = visitedNodes.get( node );
                    return relExpander.expand( node ).iterator();
                }
            };
            this.currentDepth++;
            this.sharedCurrentDepth.value++;
        }
        
        @Override
        protected LevelData fetchNextOrNull()
        {
            while ( true )
            {
                Relationship nextRel = fetchNextRelOrNull();
                if ( nextRel == null )
                {
                    return null;
                }
                
                // If we've already traversed this relationship then don't bother
                // traversing it again
//                if ( !sharedVisitedRels.add( nextRel.getId() ) )
//                {
//                    continue;
//                }
                
                Node result = nextRel.getOtherNode( this.lastParentTraverserNode );
                LevelData levelData = this.visitedNodes.get( result );
                boolean createdLevelData = false;
                if ( levelData == null )
                {
                    levelData = new LevelData( result, this.currentDepth );
                    this.visitedNodes.put( result, levelData );
                    createdLevelData = true;
                }
                
                if ( this.currentDepth < levelData.depth )
                {
                    throw new RuntimeException( "This shouldn't happen... I think" );
                }
                else if ( this.currentDepth == levelData.depth )
                {
                    for ( Path.Builder parentPath : this.lastParentLevelData.paths )
                    {
                        levelData.paths.add( parentPath.push( nextRel ) );
                    }
                }
                
                // Have we visited this node before? In that case don't add it
                // as next node to traverse
                if ( !createdLevelData )
                {
                    continue;
                }
                
                this.nextNodes.add( result );
                return levelData;
            }
        }
        
        private boolean canGoDeeper()
        {
            return this.sharedFrozenDepth.value == null && this.sharedCurrentDepth.value < maxDepth;
        }
        
        private Relationship fetchNextRelOrNull()
        {
            boolean stopped = this.stop || this.sharedStop.value;
            boolean hasComeTooFarEmptyHanded = this.sharedFrozenDepth.value != null
                    && this.sharedCurrentDepth.value > this.sharedFrozenDepth.value
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
            return this.nextRelationships.hasNext() ? this.nextRelationships.next() : null;
        }
    }
    
    private class ValueHolder<T>
    {
        private T value;
        
        ValueHolder( T initialValue )
        {
            this.value = initialValue;
        }
    }
    
    private class LevelData
    {
        private final Node node;
        private int depth;
        private final Collection<Path.Builder> paths = new ArrayList<Path.Builder>();
        
        LevelData( Node node, int depth, Path.Builder... pathsToHere )
        {
            this.node = node;
            this.depth = depth;
            this.paths.addAll( Arrays.asList( pathsToHere ) );
        }
    }
}
