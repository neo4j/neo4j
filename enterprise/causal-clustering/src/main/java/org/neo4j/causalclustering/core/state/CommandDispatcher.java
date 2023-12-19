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
package org.neo4j.causalclustering.core.state;

import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;

public interface CommandDispatcher extends AutoCloseable
{
    void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedIdAllocationRequest idAllocation, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedLockTokenRequest lockRequest, long commandIndex, Consumer<Result> callback );

    void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<Result> callback );

    @Override
    void close();
}
