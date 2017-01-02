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
package org.neo4j.causalclustering.core.state;

import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;

public interface CommandDispatcher extends AutoCloseable
{
    void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedIdAllocationRequest idAllocation, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedLockTokenRequest lockRequest, long commandIndex, Consumer<Result> callback );

    @Override
    void close();
}
