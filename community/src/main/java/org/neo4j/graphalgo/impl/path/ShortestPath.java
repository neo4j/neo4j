package org.neo4j.graphalgo.impl.path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.commons.iterator.CollectionWrapper;
import org.neo4j.commons.iterator.IteratorUtil;
import org.neo4j.commons.iterator.NestingIterator;
import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphalgo.impl.util.PathImpl.Builder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.Transaction;

/**
 * Find (all or one) simple shortest path(s) between two nodes. It starts
 * from both ends and goes one relationship at the time, alternating side
 * between each traversal. It does so to minimize the traversal overhead
 * if one side has a very large amount of relationships, but the other one
 * very few. It performs well however the graph is proportioned.
 * 
 * Relationships are traversed in the specified directions from the start node,
 * but in the reverse direction ( {@link Direction#reverse()} ) from the
 * end node. This doesn't affect {@link Direction#BOTH}.
 */
public class ShortestPath implements PathFinder<Path>
{
    private final int maxDepth;
    private final RelationshipExpander relExpander;
    
    public ShortestPath( int maxDepth, RelationshipExpander relExpander )
    {
        this.maxDepth = maxDepth;
        this.relExpander = relExpander;
    }
    
    public Iterable<Path> findAllPaths( Node start, Node end )
    {
        return internalPaths( start, end, false );
    }
    
    public Path findSinglePath( Node start, Node end )
    {
        Collection<Path> paths = internalPaths( start, end, true );
        return IteratorUtil.singleValueOrNull( paths.iterator() );
    }
    
    public Collection<Path> findPathsFromScetch( Node... someNodesAlongTheWay )
    {
        return internalPathsFromScetch( false, someNodesAlongTheWay );
    }
    
    public Path findPathFromScetch( Node... someNodesAlongTheWay )
    {
        Collection<Path> paths = internalPathsFromScetch( true, someNodesAlongTheWay );
        return IteratorUtil.singleValueOrNull( paths.iterator() );
    }
    
    private Collection<Path> internalPathsFromScetch( boolean stopAsap,
            Node... someNodesAlongTheWay )
    {
        List<PathThread> threads = new ArrayList<PathThread>();
        for ( Node[] startAndEnd : splitNodesIntoPairs( someNodesAlongTheWay ) )
        {
            PathThread thread = new PathThread( startAndEnd[0], startAndEnd[1], stopAsap );
            threads.add( thread );
            thread.start();
        }
        
        boolean allFound = true;
        for ( PathThread thread : threads )
        {
            try
            {
                thread.join();
                if ( thread.result == null || thread.result.isEmpty() )
                {
                    allFound = false;
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                // TODO
            }
        }
        
        if ( !allFound )
        {
            return Collections.emptyList();
        }
        
        Collection<Path> paths = null;
        for ( PathThread thread : threads )
        {
            paths = appendPaths( paths, thread.result );
        }
        return paths;
    }
    
    private Collection<Path> appendPaths( Collection<Path> paths, Collection<Path> step )
    {
        if ( paths == null )
        {
            return step;
        }
        Collection<Path> result = new ArrayList<Path>();
        for ( Path startPath : paths )
        {
            for ( Path pathStep : step )
            {
                result.add( merge( startPath, pathStep ) );
            }
        }
        return result;
    }

    private Path merge( Path start, Path end )
    {
        PathImpl.Builder builder = new PathImpl.Builder( start.getStartNode() );
        for ( Relationship rel : start.relationships() )
        {
            builder = builder.push( rel );
        }
        for ( Relationship rel : end.relationships() )
        {
            builder = builder.push( rel );
        }
        return builder.build();
    }

    private Collection<Node[]> splitNodesIntoPairs( Node[] someNodesAlongTheWay )
    {
        Node start = someNodesAlongTheWay[0];
        Node end = someNodesAlongTheWay[1];
        Collection<Node[]> result = new ArrayList<Node[]>();
        for ( int i = 2; end != null; i++ )
        {
            result.add( new Node[] { start, end } );
            start = end;
            end = i < someNodesAlongTheWay.length ? someNodesAlongTheWay[i] : null;
        }
        return result;
    }

    private Collection<Path> internalPaths( Node start, Node end,
            boolean stopAsap )
    {
        if ( start.equals( end ) )
        {
            return Arrays.asList( PathImpl.singular( start ) );
        }

        Map<Integer, Collection<Hit>> hits =
                new HashMap<Integer, Collection<Hit>>();
        Collection<Long> sharedVisitedRels = new HashSet<Long>();
        ValueHolder<Integer> sharedFrozenDepth = new ValueHolder<Integer>( null );
        ValueHolder<Boolean> sharedStop = new ValueHolder<Boolean>( false );
        ValueHolder<Integer> sharedCurrentDepth = new ValueHolder<Integer>( 0 );
        final DirectionData startData = new DirectionData( start,
                sharedVisitedRels, sharedFrozenDepth, sharedStop,
                sharedCurrentDepth, stopAsap, relExpander );
        final DirectionData endData = new DirectionData( end,
                sharedVisitedRels, sharedFrozenDepth, sharedStop,
                sharedCurrentDepth, stopAsap, relExpander.reversed() );
        
        while ( startData.hasNext() || endData.hasNext() )
        {
            goOneStep( startData, endData, hits, stopAsap, startData );
            goOneStep( endData, startData, hits, stopAsap, startData );
        }
        return least( hits, start, end );
    }
    
    private Collection<Path> least( Map<Integer, Collection<Hit>> hits, Node start, Node end )
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
                return hitsToPaths( depthHits, start, end );
            }
        }
    }
    
    private Collection<Path> hitsToPaths( Collection<Hit> depthHits, Node start, Node end )
    {
        Collection<Path> paths = new ArrayList<Path>();
        for ( Hit hit : depthHits )
        {
            Collection<LinkedList<Relationship>> startPaths = getPaths( hit, hit.start );
            Collection<LinkedList<Relationship>> endPaths = getPaths( hit, hit.end );
            for ( LinkedList<Relationship> startPath : startPaths )
            {
                PathImpl.Builder startBuilder = toBuilder( start, startPath );
                for ( LinkedList<Relationship> endPath : endPaths )
                {
                    PathImpl.Builder endBuilder = toBuilder( end, endPath );
                    Path path = startBuilder.build( endBuilder );
                    paths.add( path );
                }
            }
        }
        return paths;
    }
    
    private static class PathData
    {
        private final LinkedList<Relationship> rels;
        private final Node node;
        
        PathData( Node node, LinkedList<Relationship> rels )
        {
            this.rels = rels;
            this.node = node;
        }
        
        @Override
        public String toString()
        {
            return node + ":" + rels;
        }
    }

    private Collection<LinkedList<Relationship>> getPaths( Hit hit, DirectionData data )
    {
        LevelData levelData = data.visitedNodes.get( hit.connectingNode );
        if ( levelData.depth == 0 )
        {
            Collection<LinkedList<Relationship>> result = new ArrayList<LinkedList<Relationship>>();
            result.add( new LinkedList<Relationship>() );
            return result;
        }
        
        Collection<PathData> set = new ArrayList<PathData>();
        GraphDatabaseService graphDb = data.startNode.getGraphDatabase();
        for ( Long rel : levelData.relsToHere )
        {
            set.add( new PathData( hit.connectingNode, new LinkedList<Relationship>(
                    Arrays.asList( graphDb.getRelationshipById( rel ) ) ) ) );
        }
        for ( int i = 0; i < levelData.depth - 1; i++ )
        {
            // One level
            Collection<PathData> nextSet = new ArrayList<PathData>();
            for ( PathData entry : set )
            {
                // One path...
                int counter = 0;
                Node otherNode = entry.rels.getFirst().getOtherNode( entry.node );
                LevelData otherLevelData = data.visitedNodes.get( otherNode );
                for ( Long rel : otherLevelData.relsToHere )
                {
                    // ...may split into several paths
                    LinkedList<Relationship> rels = counter++ == 0 ? entry.rels : 
                        new LinkedList<Relationship>( entry.rels );
                    rels.addFirst( graphDb.getRelationshipById( rel ) );
                    nextSet.add( new PathData( otherNode, rels ) );
                }
            }
            set = nextSet;
        }
        
        return new CollectionWrapper<LinkedList<Relationship>, PathData>( set )
        {
            @Override
            protected PathData objectToUnderlyingObject( LinkedList<Relationship> list )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            protected LinkedList<Relationship> underlyingObjectToObject( PathData object )
            {
                return object.rels;
            }
        };
    }

    private Builder toBuilder( Node startNode, LinkedList<Relationship> rels )
    {
        PathImpl.Builder builder = new PathImpl.Builder( startNode );
        for ( Relationship rel : rels )
        {
            builder = builder.push( rel );
        }
        return builder;
    }

    // Few long-lived instances
    private static class Hit
    {
        private final DirectionData start;
        private final DirectionData end;
        private final Node connectingNode;
        
        Hit( DirectionData start, DirectionData end, Node connectingNode )
        {
            this.start = start;
            this.end = end;
            this.connectingNode = connectingNode;
        }
        
        @Override
        public int hashCode()
        {
            return connectingNode.hashCode();
        }
        
        @Override
        public boolean equals( Object obj )
        {
            Hit o = (Hit) obj;
            return connectingNode.equals( o.connectingNode );
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
        
        Node nextNode = directionData.next();
        LevelData otherSideHit = otherSide.visitedNodes.get( nextNode );
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
                
                DirectionData startSideData =
                        directionData == startSide ? directionData : otherSide;
                DirectionData endSideData =
                        directionData == startSide ? otherSide : directionData;
                depthHits.add( new Hit( startSideData, endSideData, nextNode ) );
            }
        }
    }
    
    // Two long-lived instances
    protected class DirectionData extends PrefetchingIterator<Node>
    {
        private final Node startNode;
        private int currentDepth;
        private Iterator<Relationship> nextRelationships;
        private final Collection<Node> nextNodes = new ArrayList<Node>();
        private Map<Node, LevelData> visitedNodes = new HashMap<Node, LevelData>();
        private Node lastParentTraverserNode;
        private final ValueHolder<Integer> sharedFrozenDepth;
        private final ValueHolder<Boolean> sharedStop;
        private final ValueHolder<Integer> sharedCurrentDepth;
        private boolean haveFoundSomething;
        private boolean stop;
        private final boolean stopAsap;
        private final RelationshipExpander expander;
        
        DirectionData( Node startNode, Collection<Long> sharedVisitedRels,
                ValueHolder<Integer> sharedFrozenDepth, ValueHolder<Boolean> sharedStop,
                ValueHolder<Integer> sharedCurrentDepth, boolean stopAsap,
                RelationshipExpander expander )
        {
            this.startNode = startNode;
            this.visitedNodes.put( startNode, new LevelData( null, 0 ) );
            this.nextNodes.add( startNode );
            this.sharedFrozenDepth = sharedFrozenDepth;
            this.sharedStop = sharedStop;
            this.sharedCurrentDepth = sharedCurrentDepth;
            this.stopAsap = stopAsap;
            this.expander = expander;
            prepareNextLevel();
        }
        
        private void prepareNextLevel()
        {
            Collection<Node> nodesToIterate = new ArrayList<Node>(
                    filterNextLevelNodes( this.nextNodes ) );
            this.nextNodes.clear();
            this.nextRelationships = new NestingIterator<Relationship, Node>(
                    nodesToIterate.iterator() )
            {
                @Override
                protected Iterator<Relationship> createNestedIterator( Node node )
                {
                    lastParentTraverserNode = node;
                    return expander.expand( node ).iterator();
                }
            };
            this.currentDepth++;
            this.sharedCurrentDepth.value++;
        }
        
        @Override
        protected Node fetchNextOrNull()
        {
            while ( true )
            {
                Relationship nextRel = fetchNextRelOrNull();
                if ( nextRel == null )
                {
                    return null;
                }
                
                Node result = nextRel.getOtherNode( this.lastParentTraverserNode );
                LevelData levelData = this.visitedNodes.get( result );
                boolean createdLevelData = false;
                if ( levelData == null )
                {
                    levelData = new LevelData( nextRel, this.currentDepth );
                    this.visitedNodes.put( result, levelData );
                    createdLevelData = true;
                }
                
                if ( this.currentDepth < levelData.depth )
                {
                    throw new RuntimeException( "This shouldn't happen... I think" );
                }
                else if ( !this.stopAsap && this.currentDepth == levelData.depth &&
                        !createdLevelData )
                {
                    levelData.addRel( nextRel );
                }
                
                // Have we visited this node before? In that case don't add it
                // as next node to traverse
                if ( !createdLevelData )
                {
                    continue;
                }
                
                this.nextNodes.add( result );
                return result;
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
    
    protected Collection<Node> filterNextLevelNodes( Collection<Node> nextNodes )
    {
        return nextNodes;
    }
    
    // Few long-lived instances
    private class ValueHolder<T>
    {
        private T value;
        
        ValueHolder( T initialValue )
        {
            this.value = initialValue;
        }
    }
    
    // Many long-lived instances
    private class LevelData
    {
        private Long[] relsToHere;
        private int depth;
        
        LevelData( Relationship relToHere, int depth )
        {
            if ( relToHere != null )
            {
                addRel( relToHere );
            }
            this.depth = depth;
        }
        
        void addRel( Relationship rel )
        {
            Long[] newRels = null;
            if ( relsToHere == null )
            {
                newRels = new Long[1];
            }
            else
            {
                newRels = new Long[relsToHere.length+1];
                System.arraycopy( relsToHere, 0, newRels, 0, relsToHere.length );
            }
            newRels[newRels.length-1] = rel.getId();
            relsToHere = newRels;
        }
    }

    // Few long-lived instances
    private class PathThread extends Thread
    {
        private final Node start;
        private final Node end;
        private final boolean stopAsap;
        private Collection<Path> result;
        
        PathThread( Node start, Node end, boolean stopAsap )
        {
            this.start = start;
            this.end = end;
            this.stopAsap = stopAsap;
        }
        
        @Override
        public void run()
        {
            Transaction tx = start.getGraphDatabase().beginTx();
            try
            {
                result = internalPaths( start, end, stopAsap );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }
}
