/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.replication.id;

import org.neo4j.kernel.impl.store.id.IdType;

public interface IdAllocationState
{
    /**
     *
     * @param idType the type of graph object whose ID is under allocation
     * @return the first unallocated entry for idType
     */
    long firstUnallocated( IdType idType );

    /**
     *
     * @param idType the type of graph object whose ID is under allocation
     * @param idRangeEnd the first unallocated entry for idType
     */
    void firstUnallocated( IdType idType, long idRangeEnd );


    /**
     *
     * @param idType The type of graph object whose ID is under allocation
     * @return start position of allocation
     */
    long lastIdRangeStart( IdType idType );

    /**
     *
     * @param idType The type of graph object whose ID is under allocation
     * @param idRangeStart start position of allocation
     */
    void lastIdRangeStart( IdType idType, long idRangeStart );

    /**
     *
     * @param idType The type of graph object whose ID is under allocation
     * @return the length of the last ID range allocated
     */
    int lastIdRangeLength( IdType idType );

    /**
     *
     * @param idType The type of graph object whose ID is under allocation
     * @param idRangeLength the length of the ID range to be allocated
     */
    void lastIdRangeLength( IdType idType, int idRangeLength );

    /**
     * @return The last set log index, which is the value last passed to {@link #logIndex(long)}
     */
    long logIndex();

    /**
     * Sets the last seen log index, which is the last log index at which a replicated value that updated this state
     * was encountered.
     * @param logIndex The value to set as the last log index at which this state was updated
     */
    void logIndex( long logIndex );
}
