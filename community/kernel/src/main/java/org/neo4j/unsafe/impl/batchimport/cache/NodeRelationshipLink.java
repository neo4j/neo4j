/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.graphdb.Direction;

/**
 * Caches of parts of node store and relationship group store. A crucial part of batch import where
 * any random access must be covered by this cache. All I/O, both read and write must be sequential.
 */
public interface NodeRelationshipLink
{
    // PHASE 1
    /**
     * Increment relationship count for {@code nodeId}.
     * @param nodeId node to increment relationship count for.
     * @return count after the increment.
     */
    int incrementCount( long nodeId );

    // PHASE 2
    long getAndPutRelationship( long nodeId, int type, Direction direction, long firstRelId,
            boolean incrementCount );

    /**
     * Used when setting node nextRel fields. Gets the first relationship for this node,
     * or the first relationship group id (where it it first visits all the groups before returning the first one).
     */
    long getFirstRel( long nodeId, GroupVisitor visitor );

    boolean isDense( long nodeId );

    void clearRelationships();

    int getCount( long nodeId, int type, Direction direction );

    public interface GroupVisitor
    {
        /**
         * @param nodeId
         * @return the relationship group id created.
         */
        long visit( long nodeId, int type, long next, long out, long in, long loop );
    }

    public static final GroupVisitor NO_GROUP_VISITOR = new GroupVisitor()
    {
        @Override
        public long visit( long nodeId, int type, long next, long out, long in, long loop )
        {
            return -1;
        }
    };
}
