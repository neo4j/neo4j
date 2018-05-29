/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
