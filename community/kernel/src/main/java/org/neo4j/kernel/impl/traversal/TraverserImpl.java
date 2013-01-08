/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

class TraverserImpl extends AbstractTraverser
{
    final TraversalDescriptionImpl description;
    final Iterable<Node> startNodes;

    TraverserImpl( TraversalDescriptionImpl description, Iterable<Node> startNodes )
    {
        this.description = description;
        this.startNodes = startNodes;
    }

    protected Iterator<Path> instantiateIterator()
    {
        TraverserIterator iterator = new TraverserIterator( description.uniqueness.create( description.uniquenessParameter ),
                description.expander, description.branchOrdering, description.evaluator,
                startNodes, description.initialState );
        return description.sorting != null ? new SortingTraverserIterator( this, iterator ) : iterator;
    }
}
