/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

public class StoreCopyFinishedResponse
{
    public enum Status
    {
        SUCCESS,
        E_STORE_ID_MISMATCH
    }

    private final Status status;
    private final long lastCommittedTxBeforeStoreCopy;

    public StoreCopyFinishedResponse( Status status,
            long lastCommittedTxBeforeStoreCopy )
    {
        this.status = status;
        this.lastCommittedTxBeforeStoreCopy = lastCommittedTxBeforeStoreCopy;
    }

    long lastCommittedTxBeforeStoreCopy()
    {
        return lastCommittedTxBeforeStoreCopy;
    }

    Status status()
    {
        return status;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        StoreCopyFinishedResponse that = (StoreCopyFinishedResponse) o;
        return lastCommittedTxBeforeStoreCopy == that.lastCommittedTxBeforeStoreCopy &&
               status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status, lastCommittedTxBeforeStoreCopy );
    }
}
