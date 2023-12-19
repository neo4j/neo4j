/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.Optional;

class CommitState
{
    private final long metaDataStoreIndex;
    private final Long transactionLogIndex;

    CommitState( long metaDataStoreIndex )
    {
        this.metaDataStoreIndex = metaDataStoreIndex;
        this.transactionLogIndex = null;
    }

    CommitState( long metaDataStoreIndex, long transactionLogIndex )
    {
        assert transactionLogIndex >= metaDataStoreIndex;

        this.metaDataStoreIndex = metaDataStoreIndex;
        this.transactionLogIndex = transactionLogIndex;
    }

    long metaDataStoreIndex()
    {
        return metaDataStoreIndex;
    }

    Optional<Long> transactionLogIndex()
    {
        return Optional.ofNullable( transactionLogIndex );
    }

    @Override
    public String toString()
    {
        return "CommitState{" + "metaDataStoreIndex=" + metaDataStoreIndex + ", transactionLogIndex=" + transactionLogIndex + '}';
    }
}
