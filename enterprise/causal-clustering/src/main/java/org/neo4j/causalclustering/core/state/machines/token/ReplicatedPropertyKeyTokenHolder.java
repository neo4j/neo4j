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
package org.neo4j.causalclustering.core.state.machines.token;

import org.neo4j.causalclustering.core.replication.RaftReplicator;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.storageengine.api.Token;

public class ReplicatedPropertyKeyTokenHolder extends ReplicatedTokenHolder<Token> implements
        PropertyKeyTokenHolder
{
    public ReplicatedPropertyKeyTokenHolder( TokenRegistry<Token> registry, RaftReplicator replicator,
            IdGeneratorFactory idGeneratorFactory, Dependencies dependencies )
    {
        super( registry, replicator, idGeneratorFactory, IdType.PROPERTY_KEY_TOKEN, dependencies, TokenType.PROPERTY );
    }

    @Override
    protected void createToken( TransactionState txState, String tokenName, int tokenId )
    {
        txState.propertyKeyDoCreateForName( tokenName, tokenId );
    }
}
