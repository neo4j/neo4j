/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.state.machines.token;

import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.storageengine.api.Token;

public class ReplicatedLabelTokenHolder extends ReplicatedTokenHolder<Token> implements LabelTokenHolder
{
    public ReplicatedLabelTokenHolder( TokenRegistry<Token> registry, Replicator replicator,
            IdGeneratorFactory idGeneratorFactory, Dependencies dependencies )
    {
        super( registry, replicator, idGeneratorFactory, IdType.LABEL_TOKEN, dependencies, TokenType.LABEL );
    }

    @Override
    protected void createToken( TransactionState txState, String tokenName, int tokenId )
    {
        txState.labelDoCreateForName( tokenName, tokenId );
    }
}
