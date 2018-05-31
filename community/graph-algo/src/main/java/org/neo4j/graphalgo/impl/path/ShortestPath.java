/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.path;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphalgo.impl.util.PathImpl.Builder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.NestingResourceIterator;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.monitoring.Monitors;

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
    public final int NULL = -1;
    private final int maxDepth;
    private final int maxResultCount;
    private final PathExpander expander;
    private Metadata lastMetadata;
    private ShortestPathPredicate predicate;
    private DataMonitor dataMonitor;

    public interface ShortestPathPredicate
    {
        boolean test( Path path );
    }

    /**
     * Constructs a new shortest path algorithm.
     * @param maxDepth the maximum depth for the traversal. Returned paths
     * will never have a greater {@link Path#length()} than {@code maxDepth}.
     * @param expander the {@link PathExpander} to use for deciding
     * which relationships to expand for each {@link Node}.
     */
    public ShortestPath( int maxDepth, PathExpander expander )
    {
        this( maxDepth, expander, Integer.MAX_VALUE );
    }

    public ShortestPath( int maxDepth, PathExpander expander, ShortestPathPredicate predicate )
    {
        this( maxDepth, expander );
        this.predicate = predicate;
    }

    /**
     * Constructs a new shortest path algorithm.
     * @param maxDepth the maximum depth for the traversal. Returned paths
     * will never have a greater {@link Path#length()} than {@code maxDepth}.
     * @param expander the {@link PathExpander} to use for deciding
     * which relationships to expand for each {@link Node}.
     * @param maxResultCount the maximum number of hits to return. If this number
     * of hits are encountered the traversal will stop.
     */
    public ShortestPath( int maxDepth, PathExpander expander, int maxResultCount )
    {
        this.maxDepth = maxDepth;
        this.expander = expander;
        this.maxResultCount = maxResultCount;
    }

    @Override
    public Iterable<Path> findAllPaths( Node start, Node end )
    {
        return internalPaths( start, end, false );
    }

    @Override
    public Path findSinglePath( Node start, Node end )
    {
        Iterator<Path> paths = internalPaths( start, end, true ).iterator();
        return paths.hasNext() ? paths.next() : null;
    }

    private void resolveMonitor( Node node )
    {
        if ( dataMonitor == null )
        {
            GraphDatabaseService service = node.getGraphDatabase();
            if ( service instanceof GraphDatabaseFacade )
            {
                Monitors monitors = ((GraphDatabaseFacade) service).getDependencyResolver().resolveDependency( Monitors.class );
                dataMonitor = monitors.newMonitor( DataMonitor.class );
            }
        }
    }

    private Iterable<Path> internalPaths( Node start, Node end, boolean stopAsap )
    {
        lastMetadata = new Metadata();
        if ( start.equals( end ) )
        {
            return filterPaths(Collections.singletonList( PathImpl.singular( start ) ));
        }
        Hits hits = new Hits();
        Collection<Long> sharedVisitedRels = new HashSet<>();
        MutableInt sharedFrozenDepth = new MutableInt( NULL ); // ShortestPathLengthSoFar
        MutableBoolean sharedStop = new MutableBoolean();
        MutableInt sharedCurrentDepth = new MutableInt( 0 );
        try ( DirectionData startData = new DirectionData( start, sharedVisitedRels,
                sharedFrozenDepth, sharedStop, sharedCurrentDepth, expander );
              DirectionData endData = new DirectionData( end, sharedVisitedRels, sharedFrozenDepth,
                      sharedStop, sharedCurrentDepth, expander.reverse() ) )
        {
            while ( startData.hasNext() || endData.hasNext() )
            {
                goOneStep( startData, endData, hits, startData, stopAsap );
                goOneStep( endData, startData, hits, startData, stopAsap );
            }
            Collection<Hit> least = hits.least();
            return least != null ? filterPaths( hitsToPaths( least, start, end, stopAsap, maxResultCount ) ) : Collections.emptyList();
        }
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastMetadata;
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
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null || getClass() != obj.getClass() )
            {
                return false;
            }
            Hit o = (Hit) obj;
            return connectingNode.equals( o.connectingNode );
        }
    }

    private void goOneStep( DirectionData directionData, DirectionData otherSide, Hits hits, DirectionData startSide,
            boolean stopAsap )
    {
        if ( !directionData.hasNext() )
        {
            // We can not go any deeper from this direction. Possibly disconnected nodes.
            otherSide.finishCurrentLayerThenStop = true;
            return;
        }
        Node nextNode = directionData.next();
        LevelData otherSideHit = otherSide.visitedNodes.get( nextNode );
        if ( otherSideHit != null )
        {
            // This is a hit
            int depth = directionData.currentDepth + otherSideHit.depth;

            if ( directionData.sharedFrozenDepth.intValue() == NULL )
            {
                directionData.sharedFrozenDepth.setValue( depth );
            }
            if ( depth <= directionData.sharedFrozenDepth.intValue() )
            {
                directionData.haveFoundSomething = true;
                if ( depth < directionData.sharedFrozenDepth.intValue() )
                {
                    directionData.sharedFrozenDepth.setValue( depth );
                    // TODO Is it really ok to just stop the other side here?
                    // I'm basing that decision on that it was the other side
                    // which found the deeper paths (correct assumption?)
                    otherSide.stop = true;
                }
                // Add it to the list of hits
                DirectionData startSideData = directionData == startSide ? directionData : otherSide;
                DirectionData endSideData = directionData == startSide ? otherSide : directionData;
                Hit hit = new Hit( startSideData, endSideData, nextNode );
                Node start = startSide.startNode;
                Node end = (startSide == directionData) ? otherSide.startNode : directionData.startNode;
                monitorData( startSide, (otherSide == startSide) ? directionData : otherSide, nextNode );
                // NOTE: Applying the filter-condition could give the wrong results with allShortestPaths,
                // so only use it for singleShortestPath
                if ( !stopAsap || filterPaths( hitToPaths( hit, start, end, stopAsap ) ).size() > 0 )
                {
                    if ( hits.add( hit, depth ) >= maxResultCount )
                    {
                        directionData.stop = true;
                        otherSide.stop = true;
                        lastMetadata.paths++;
                    }
                    else if ( stopAsap )
                    {   // This side found a hit, but wait for the other side to complete its current depth
                        // to see if it finds a shorter path. (i.e. stop this side and freeze the depth).
                        // but only if the other side has not stopped, otherwise we might miss shorter paths
                        if ( otherSide.stop )
                        {
                            return;
                        }
                        directionData.stop = true;
                    }
                }
                else
                {
                    directionData.haveFoundSomething = false;
                    directionData.sharedFrozenDepth.setValue( NULL );
                    otherSide.stop = false;
                }
            }
        }
    }

    private void monitorData( DirectionData directionData, DirectionData otherSide, Node connectingNode )
    {
        resolveMonitor( directionData.startNode );
        if ( dataMonitor != null )
        {
            dataMonitor.monitorData( directionData.visitedNodes, directionData.nextNodes, otherSide.visitedNodes,
                    otherSide.nextNodes, connectingNode );
        }
    }

    private Collection<Path> filterPaths( Collection<Path> paths )
    {
        if ( predicate == null )
        {
            return paths;
        }
        else
        {
            Collection<Path> filteredPaths = new ArrayList<>();
            for ( Path path : paths )
            {
                if ( predicate.test( path ) )
                {
                    filteredPaths.add( path );
                }
            }
            return filteredPaths;
        }
    }

    public interface DataMonitor
    {
        void monitorData( Map<Node,LevelData> theseVisitedNodes, Collection<Node> theseNextNodes,
                Map<Node,LevelData> thoseVisitedNodes, Collection<Node> thoseNextNodes, Node connectingNode );
    }

    // Two long-lived instances
    private class DirectionData extends PrefetchingResourceIterator<Node>
    {
        private boolean finishCurrentLayerThenStop;
        private final Node startNode;
        private int currentDepth;
        private ResourceIterator<Relationship> nextRelationships;
        private final Collection<Node> nextNodes = new ArrayList<>();
        private final Map<Node,LevelData> visitedNodes = new HashMap<>();
        private final Collection<Long> sharedVisitedRels;
        private final DirectionDataPath lastPath;
        private final MutableInt sharedFrozenDepth;
        private final MutableBoolean sharedStop;
        private final MutableInt sharedCurrentDepth;
        private boolean haveFoundSomething;
        private boolean stop;
        private final PathExpander expander;

        DirectionData( Node startNode, Collection<Long> sharedVisitedRels, MutableInt sharedFrozenDepth,
                MutableBoolean sharedStop, MutableInt sharedCurrentDepth, PathExpander expander )
        {
            this.startNode = startNode;
            this.visitedNodes.put( startNode, new LevelData( null, 0 ) );
            this.nextNodes.add( startNode );
            this.sharedFrozenDepth = sharedFrozenDepth;
            this.sharedStop = sharedStop;
            this.sharedCurrentDepth = sharedCurrentDepth;
            this.expander = expander;
            this.sharedVisitedRels = sharedVisitedRels;
            this.lastPath = new DirectionDataPath( startNode );
            if ( sharedCurrentDepth.intValue() < maxDepth )
            {
                prepareNextLevel();
            }
            else
            {
                this.nextRelationships = Iterators.emptyResourceIterator();
            }
        }

        private void prepareNextLevel()
        {
            Collection<Node> nodesToIterate = new ArrayList<>( this.nextNodes );
            this.nextNodes.clear();
            this.lastPath.setLength( currentDepth );
            closeRelationshipsIterator();
            this.nextRelationships = new NestingResourceIterator<Relationship,Node>( nodesToIterate.iterator() )
            {
                @Override
                protected ResourceIterator<Relationship> createNestedIterator( Node node )
                {
                    lastPath.setEndNode( node );
                    return Iterators.asResourceIterator( expander.expand( lastPath, BranchState.NO_STATE ).iterator() );
                }
            };
            this.currentDepth++;
            this.sharedCurrentDepth.increment();
        }

        private void closeRelationshipsIterator()
        {
            if ( this.nextRelationships != null )
            {
                this.nextRelationships.close();
            }
        }

        @Override
        public void close()
        {
            closeRelationshipsIterator();
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

                Node result = nextRel.getOtherNode( this.lastPath.endNode() );

                if ( filterNextLevelNodes( result ) != null )
                {
                    lastMetadata.rels++;

                    LevelData levelData = this.visitedNodes.get( result );
                    if ( levelData == null )
                    {
                        levelData = new LevelData( nextRel, this.currentDepth );
                        this.visitedNodes.put( result, levelData );
                        this.nextNodes.add( result );
                        return result;
                    }
                    else if ( this.currentDepth == levelData.depth )
                    {
                        levelData.addRel( nextRel );
                    }
                }
            }
        }

        private boolean canGoDeeper()
        {
            return (this.sharedFrozenDepth.intValue() == NULL) && (this.sharedCurrentDepth.intValue() < maxDepth) &&
                   !finishCurrentLayerThenStop;
        }

        private Relationship fetchNextRelOrNull()
        {
            if ( this.stop || this.sharedStop.booleanValue() )
            {
                return null;
            }
            boolean hasComeTooFarEmptyHanded = (this.sharedFrozenDepth.intValue() != NULL) &&
                                               (this.sharedCurrentDepth.intValue() > this.sharedFrozenDepth.intValue()) &&
                                               !this.haveFoundSomething;
            if ( hasComeTooFarEmptyHanded )
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

    // Two long-lived instances
    private static class DirectionDataPath implements Path
    {
        private final Node startNode;
        private Node endNode;
        private int length;

        DirectionDataPath( Node startNode )
        {
            this.startNode = startNode;
            this.endNode = startNode;
            this.length = 0;
        }

        void setEndNode( Node endNode )
        {
            this.endNode = endNode;
        }

        void setLength( int length )
        {
            this.length = length;
        }

        @Override
        public Node startNode()
        {
            return startNode;
        }

        @Override
        public Node endNode()
        {
            return endNode;
        }

        @Override
        public Relationship lastRelationship()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Relationship> relationships()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Relationship> reverseRelationships()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Node> nodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Node> reverseNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public Iterator<PropertyContainer> iterator()
        {
            throw new UnsupportedOperationException();
        }
    }

    protected Node filterNextLevelNodes( Node nextNode )
    {
        // We need to be able to override this method from Cypher, so it must exist in this concrete class.
        // And we also need it to do nothing but still work when not overridden.
        return nextNode;
    }

    // Many long-lived instances
    public static class LevelData
    {
        private long[] relsToHere;
        public final int depth;

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
            long[] newRels = null;
            if ( relsToHere == null )
            {
                newRels = new long[1];
            }
            else
            {
                newRels = new long[relsToHere.length + 1];
                System.arraycopy( relsToHere, 0, newRels, 0, relsToHere.length );
            }
            newRels[newRels.length - 1] = rel.getId();
            relsToHere = newRels;
        }
    }

    // One long lived instance
    private static class Hits
    {
        private final MutableIntObjectMap<Collection<Hit>> hits = new IntObjectHashMap<>();
        private int lowestDepth;
        private int totalHitCount;

        int add( Hit hit, int atDepth )
        {
            Collection<Hit> depthHits = hits.getIfAbsentPut( atDepth, HashSet::new );
            if ( depthHits.add( hit ) )
            {
                totalHitCount++;
            }
            if ( lowestDepth == 0 || atDepth < lowestDepth )
            {
                lowestDepth = atDepth;
            }
            return totalHitCount;
        }

        Collection<Hit> least()
        {
            return hits.get( lowestDepth );
        }
    }

    // Methods for converting data representing paths to actual Path instances.
    // It's rather tricky just because this algo stores as little info as possible
    // required to build paths from hit information.
    private static class PathData
    {
        private final LinkedList<Relationship> rels;
        private final Node node;

        PathData( Node node, LinkedList<Relationship> rels )
        {
            this.rels = rels;
            this.node = node;
        }
    }

    private static Collection<Path> hitsToPaths( Collection<Hit> depthHits, Node start, Node end, boolean stopAsap, int maxResultCount )
    {
        LinkedHashMap<String,Path> paths = new LinkedHashMap<>();
        for ( Hit hit : depthHits )
        {
            for ( Path path : hitToPaths( hit, start, end, stopAsap ) )
            {
                paths.put( path.toString(), path );
                if ( paths.size() >= maxResultCount )
                {
                    break;
                }
            }
        }
        return paths.values();
    }

    private static Collection<Path> hitToPaths( Hit hit, Node start, Node end, boolean stopAsap )
    {
        Collection<Path> paths = new ArrayList<>();
        Iterable<LinkedList<Relationship>> startPaths = getPaths( hit.connectingNode, hit.start, stopAsap );
        Iterable<LinkedList<Relationship>> endPaths = getPaths( hit.connectingNode, hit.end, stopAsap );
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
        return paths;
    }

    private static Iterable<LinkedList<Relationship>> getPaths( Node connectingNode, DirectionData data,
            boolean stopAsap )
    {
        LevelData levelData = data.visitedNodes.get( connectingNode );
        if ( levelData.depth == 0 )
        {
            Collection<LinkedList<Relationship>> result = new ArrayList<>();
            result.add( new LinkedList<>() );
            return result;
        }
        Collection<PathData> set = new ArrayList<>();
        GraphDatabaseService graphDb = data.startNode.getGraphDatabase();
        for ( long rel : levelData.relsToHere )
        {
            set.add( new PathData( connectingNode, new LinkedList<>( Arrays.asList( graphDb
                    .getRelationshipById( rel ) ) ) ) );
            if ( stopAsap )
            {
                break;
            }
        }
        for ( int i = 0; i < levelData.depth - 1; i++ )
        {
            // One level
            Collection<PathData> nextSet = new ArrayList<>();
            for ( PathData entry : set )
            {
                // One path...
                Node otherNode = entry.rels.getFirst().getOtherNode( entry.node );
                LevelData otherLevelData = data.visitedNodes.get( otherNode );
                int counter = 0;
                for ( long rel : otherLevelData.relsToHere )
                {
                    // ...may split into several paths
                    LinkedList<Relationship> rels = ++counter == otherLevelData.relsToHere.length ?
                    // This is a little optimization which reduces number of
                    // lists being copied
                            entry.rels
                            : new LinkedList<>( entry.rels );
                    rels.addFirst( graphDb.getRelationshipById( rel ) );
                    nextSet.add( new PathData( otherNode, rels ) );
                    if ( stopAsap )
                    {
                        break;
                    }
                }
            }
            set = nextSet;
        }
        return new IterableWrapper<LinkedList<Relationship>,PathData>( set )
        {
            @Override
            protected LinkedList<Relationship> underlyingObjectToObject( PathData object )
            {
                return object.rels;
            }
        };
    }

    private static Builder toBuilder( Node startNode, LinkedList<Relationship> rels )
    {
        PathImpl.Builder builder = new PathImpl.Builder( startNode );
        for ( Relationship rel : rels )
        {
            builder = builder.push( rel );
        }
        return builder;
    }

    private static class Metadata implements TraversalMetadata
    {
        private int rels;
        private int paths;

        @Override
        public int getNumberOfPathsReturned()
        {
            return paths;
        }

        @Override
        public int getNumberOfRelationshipsTraversed()
        {
            return rels;
        }
    }
}
