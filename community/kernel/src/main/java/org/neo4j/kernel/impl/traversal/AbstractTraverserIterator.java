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

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;

abstract class AbstractTraverserIterator extends PrefetchingResourceIterator<Path> implements TraverserIterator
{
    protected int numberOfPathsReturned;
    protected int numberOfRelationshipsTraversed;
    private final Resource resource;

    protected AbstractTraverserIterator( Resource resource )
    {
        this.resource = resource;
    }

    @Override
    public int getNumberOfPathsReturned()
    {
        return numberOfPathsReturned;
    }

    @Override
    public int getNumberOfRelationshipsTraversed()
    {
        return numberOfRelationshipsTraversed;
    }

    @Override
    public void relationshipTraversed()
    {
        numberOfRelationshipsTraversed++;
    }

    @Override
    public void unnecessaryRelationshipTraversed()
    {
        numberOfRelationshipsTraversed++;
    }

    @Override
    public void close()
    {
        resource.close();
    }
}
