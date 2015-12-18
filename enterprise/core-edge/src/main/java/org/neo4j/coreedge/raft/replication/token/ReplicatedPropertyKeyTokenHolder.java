/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.token;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.util.Dependencies;

public class ReplicatedPropertyKeyTokenHolder extends ReplicatedTokenHolder<Token,PropertyKeyTokenRecord> implements PropertyKeyTokenHolder
{
    public ReplicatedPropertyKeyTokenHolder( Replicator replicator, IdGeneratorFactory idGeneratorFactory, Dependencies dependencies )
    {
        super( replicator, idGeneratorFactory, IdType.PROPERTY_KEY_TOKEN, dependencies, new Token.Factory(), TokenType.PROPERTY );
    }

    @Override
    protected RecordAccess.Loader<Integer,PropertyKeyTokenRecord,Void> resolveLoader( TokenStore<PropertyKeyTokenRecord,Token> tokenStore )
    {
        return Loaders.propertyKeyTokenLoader( tokenStore );
    }

    @Override
    protected TokenStore<PropertyKeyTokenRecord,Token> resolveStore()
    {
        return dependencies.resolveDependency( NeoStores.class ).getPropertyKeyTokenStore();
    }

    protected Command.TokenCommand<PropertyKeyTokenRecord> createCommand( PropertyKeyTokenRecord before,
            PropertyKeyTokenRecord after )
    {
        return new Command.PropertyKeyTokenCommand( before, after );
    }
}
