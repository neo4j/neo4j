/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.traversal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;

class SortingTraverserIterator extends PrefetchingResourceIterator<Path> implements TraverserIterator
{
    private final Comparator<? super Path> sortingStrategy;
    private final MonoDirectionalTraverserIterator source;
    private final Resource resource;
    private Iterator<Path> sortedResultIterator;

    SortingTraverserIterator( Resource resource, Comparator<? super Path> sortingStrategy, MonoDirectionalTraverserIterator source )
    {
        this.resource = resource;
        this.sortingStrategy = sortingStrategy;
        this.source = source;
    }

    @Override
    public int getNumberOfPathsReturned()
    {
        return source.getNumberOfPathsReturned();
    }

    @Override
    public int getNumberOfRelationshipsTraversed()
    {
        return source.getNumberOfRelationshipsTraversed();
    }

    @Override
    public void relationshipTraversed()
    {
        source.relationshipTraversed();
    }

    @Override
    public void unnecessaryRelationshipTraversed()
    {
        source.unnecessaryRelationshipTraversed();
    }

    @Override
    public boolean isUniqueFirst( TraversalBranch branch )
    {
        return source.isUniqueFirst( branch );
    }

    @Override
    public boolean isUnique( TraversalBranch branch )
    {
        return source.isUnique( branch );
    }

    @Override
    public Evaluation evaluate( TraversalBranch branch, BranchState state )
    {
        return source.evaluate( branch, state );
    }

    @Override
    protected Path fetchNextOrNull()
    {
        if ( sortedResultIterator == null )
        {
            sortedResultIterator = fetchAndSortResult();
        }
        return sortedResultIterator.hasNext() ? sortedResultIterator.next() : null;
    }

    private Iterator<Path> fetchAndSortResult()
    {
        List<Path> result = new ArrayList<>();
        while ( source.hasNext() )
        {
            result.add( source.next() );
        }
        Collections.sort( result, sortingStrategy );
        return result.iterator();
    }

    @Override
    public void close()
    {
        resource.close();
    }
}