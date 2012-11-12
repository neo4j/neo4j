/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

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
        TraversalDescription base = Traversal.description().uniqueness(
                Uniqueness.RELATIONSHIP_PATH ).order(
                new BranchOrderingPolicy()
                {
                    public BranchSelector create( TraversalBranch startSource )
                    {
                        return new LiteDepthFirstSelector( startSource,
                                startThreshold );
                    }
                } );
        final int firstHalf = onDepth / 2;
        Traverser startTraverser = base.prune(
                Traversal.pruneAfterDepth( firstHalf ) ).expand(
                expander ).filter( new Predicate<Path>()
        {
            public boolean accept( Path item )
            {
                return item.length() == firstHalf;
            }
        } ).traverse( start );
        final int secondHalf = onDepth - firstHalf;
        Traverser endTraverser = base.prune(
                Traversal.pruneAfterDepth( secondHalf ) ).expand(
                expander.reversed() ).filter( new Predicate<Path>()
        {
            public boolean accept( Path item )
            {
                return item.length() == secondHalf;
            }
        } ).traverse( end );

        final Iterator<Path> startIterator = startTraverser.iterator();
        final Iterator<Path> endIterator = endTraverser.iterator();

        final Map<Node, Visit> visits = new HashMap<Node, Visit>();
        return new PrefetchingIterator<Path>()
        {
            @Override
            protected Path fetchNextOrNull()
            {
                Path[] found = null;
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

    private Path toPath( Path[] found, Node start )
    {
        Path startPath = found[0];
        Path endPath = found[1];
        if ( !startPath.startNode().equals( start ) )
        {
            Path tmpPath = startPath;
            startPath = endPath;
            endPath = tmpPath;
        }
        return toBuilder( startPath ).build( toBuilder( endPath ) );
    }

    private Builder toBuilder( Path path )
    {
        Builder builder = new Builder( path.startNode() );
        for ( Relationship rel : path.relationships() )
        {
            builder = builder.push( rel );
        }
        return builder;
    }

    private Path[] goOneStep( Node node, Iterator<Path> visitor,
            Map<Node, Visit> visits )
    {
        if ( !visitor.hasNext() )
        {
            return null;
        }
        Path position = visitor.next();
        Visit visit = visits.get( position.endNode() );
        if ( visit != null )
        {
            if ( visitor != visit.visitor )
            {
                return new Path[] { visit.position, position };
            }
        }
        else
        {
            visits.put( position.endNode(), new Visit( position, visitor ) );
        }
        return null;
    }

    private static class Visit
    {
        private final Path position;
        private final Iterator<Path> visitor;

        Visit( Path position, Iterator<Path> visitor )
        {
            this.position = position;
            this.visitor = visitor;
        }
    }
}
