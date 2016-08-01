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
package org.neo4j.coreedge.core.state.machines.token;

import org.neo4j.coreedge.core.replication.RaftReplicator;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.util.Dependencies;

public class ReplicatedRelationshipTypeTokenHolder extends
        ReplicatedTokenHolder<RelationshipTypeToken> implements RelationshipTypeTokenHolder
{
    public ReplicatedRelationshipTypeTokenHolder(
            TokenRegistry<RelationshipTypeToken> registry,
            RaftReplicator replicator,
            IdGeneratorFactory idGeneratorFactory, Dependencies dependencies, Long timeoutMillis )
    {
        super( registry, replicator, idGeneratorFactory, IdType.RELATIONSHIP_TYPE_TOKEN, dependencies,
                TokenType.RELATIONSHIP, timeoutMillis );
    }

    @Override
    protected void createToken( TransactionState txState, String tokenName, int tokenId )
    {
        txState.relationshipTypeDoCreateForName( tokenName, tokenId );
    }
}
