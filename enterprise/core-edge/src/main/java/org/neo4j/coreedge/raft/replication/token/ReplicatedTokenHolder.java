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
package org.neo4j.coreedge.raft.replication.token;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.NonUniqueTokenException;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.lock.ResourceLocker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class ReplicatedTokenHolder<TOKEN extends Token> implements TokenHolder<TOKEN>
{
    protected final Dependencies dependencies;

    private final Replicator replicator;
    private final TokenRegistry<TOKEN> tokenRegistry;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType tokenIdType;
    private final TokenType type;
    private final long timeoutMillis;

    // TODO: Clean up all the resolving, which now happens every time with special selection strategies.

    public ReplicatedTokenHolder( TokenRegistry<TOKEN> tokenRegistry, Replicator replicator,
                                  IdGeneratorFactory idGeneratorFactory, IdType tokenIdType,
                                  Dependencies dependencies, TokenType type,
                                  long timeoutMillis )
    {
        this.replicator = replicator;
        this.tokenRegistry = tokenRegistry;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenIdType = tokenIdType;
        this.type = type;
        this.dependencies = dependencies;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void setInitialTokens( List<TOKEN> tokens ) throws NonUniqueTokenException
    {
        tokenRegistry.setInitialTokens( tokens );
    }

    @Override
    public void addToken( TOKEN token ) throws NonUniqueTokenException
    {
        tokenRegistry.addToken( token );
    }

    @Override
    public int getOrCreateId( String tokenName )
    {
        Integer tokenId = tokenRegistry.getId( tokenName );
        if ( tokenId != null )
        {
            return tokenId;
        }

        return requestToken( tokenName );
    }

    private int requestToken( String tokenName )
    {
        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( type, tokenName, createCommands( tokenName ) );
        try
        {
            Future<Object> future = replicator.replicate( tokenRequest, true );
            return (int) future.get( timeoutMillis, MILLISECONDS );
        }
        catch ( InterruptedException | TimeoutException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Could not create token", e );
        }
        catch ( ExecutionException e )
        {
            throw new IllegalStateException( e );
        }
    }

    private byte[] createCommands( String tokenName )
    {
        StorageEngine storageEngine = dependencies.resolveDependency( StorageEngine.class );
        Collection<StorageCommand> commands = new ArrayList<>();
        TransactionState txState = new TxState();
        int tokenId = Math.toIntExact( idGeneratorFactory.get( tokenIdType ).nextId() );
        createToken( txState, tokenName, tokenId );
        try ( StorageStatement statement = storageEngine.storeReadLayer().newStatement() )
        {
            storageEngine.createCommands( commands, txState, statement, ResourceLocker.NONE, Long.MAX_VALUE );
        }
        catch ( CreateConstraintFailureException | TransactionFailureException | ConstraintValidationKernelException e )
        {
            throw new RuntimeException( "Unable to create token '" + tokenName + "'", e );
        }

        return ReplicatedTokenRequestSerializer.commandBytes( commands );
    }

    protected abstract void createToken( TransactionState txState, String tokenName, int tokenId );

    @Override
    public TOKEN getTokenById( int id ) throws TokenNotFoundException
    {
        TOKEN result = getTokenByIdOrNull( id );
        if ( result == null )
        {
            throw new TokenNotFoundException( "Token for id " + id );
        }
        return result;
    }

    @Override
    public TOKEN getTokenByIdOrNull( int id )
    {
        return tokenRegistry.getToken( id );
    }

    @Override
    public int getIdByName( String name )
    {
        Integer id = tokenRegistry.getId( name );
        if ( id == null )
        {
            return NO_ID;
        }
        return id;
    }

    @Override
    public Iterable<TOKEN> getAllTokens()
    {
        return tokenRegistry.allTokens();
    }

    @Override
    public int size()
    {
        return tokenRegistry.size();
    }
}
