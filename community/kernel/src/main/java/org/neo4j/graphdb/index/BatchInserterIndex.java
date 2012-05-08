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
package org.neo4j.graphdb.index;

import java.util.Map;

import org.neo4j.kernel.impl.batchinsert.BatchInserter;

/**
 * The {@link BatchInserter} version of {@link Index}. Additions/updates to a
 * {@link BatchInserterIndex} doesn't necessarily gets added to the actual index
 * immediately, but are instead forced to be written when the index is shut
 * down, {@link BatchInserterIndexProvider#shutdown()}.
 * 
 * To guarantee additions/updates are seen by {@link #updateOrAdd(long, Map)},
 * {@link #get(String, Object)}, {@link #query(String, Object)} and
 * {@link #query(Object)} a call to {@link #flush()} must be made prior to
 * calling such a method. This enables implementations more flexibility in
 * making for performance optimizations.
 * 
 * @deprecated this interface has been moved to
 *             {@link org.neo4j.unsafe.batchinsert.BatchInserterIndex} as of
 *             version 1.7
 */
public interface BatchInserterIndex extends
        org.neo4j.unsafe.batchinsert.BatchInserterIndex
{
}
