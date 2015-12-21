/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.countStore;

import java.util.Map;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

/**
 * An interface for a count store which allows for concurrent updates to the elements of the store, retrieving
 * elements from the store, and for retrieving snapshots of the store given the desired transaction ID to take the
 * snapshot from. Update transactions may be applied out of order, therefore the returned snapshot may be of the
 * count store at a higher transaction ID value than the requested value.
 */
public interface CountsStore
{
    long[] get( CountsKey key );

    void updateAll( long txId, Map<CountsKey,long[]> updates );

    /**
     * This method is thread safe w.r.t updates to the CountStore, but not for performing concurrent snapshots.
     * You may request a snapshot while updates are being applied, and will receive a valid snapshot of the
     * CountStore, but requesting a snapshot before the last call to this method returns will cause an exception.
     *
     * @param txId The desired transaction ID to snapshot.
     * @return A snapshot of the count store with a txId greater than or equal to the txId parameter.
     * @throws IllegalStateException If called before the previous invocation of this method returns.
     */
    CountsSnapshot snapshot( long txId );
}
