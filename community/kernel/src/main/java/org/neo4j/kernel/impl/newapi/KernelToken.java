/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import java.util.Iterator;
import java.util.function.IntSupplier;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;

import static org.neo4j.internal.kernel.api.TokenWrite.checkValidTokenName;

public class KernelToken implements Token
{
    private final StorageReader store;
    private final CommandCreationContext commandCreationContext;
    private final KernelTransactionImplementation ktx;
    private final TokenHolders tokenHolders;

    public KernelToken( StorageReader store, CommandCreationContext commandCreationContext, KernelTransactionImplementation ktx, TokenHolders tokenHolders )
    {
        this.store = store;
        this.commandCreationContext = commandCreationContext;
        this.ktx = ktx;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws KernelException
    {
        return getOrCreateForName( tokenHolders.labelTokens(), PrivilegeAction.CREATE_LABEL, labelName );
    }

    @Override
    public void labelGetOrCreateForNames( String[] labelNames, int[] labelIds ) throws KernelException
    {
        getOrCreateForNames( tokenHolders.labelTokens(), PrivilegeAction.CREATE_LABEL, labelNames, labelIds );
    }

    @Override
    public int labelCreateForName( String labelName, boolean internal ) throws KernelException
    {
        ktx.assertOpen();
        TransactionState txState = ktx.txState();
        int id = reserveTokenId( commandCreationContext::reserveLabelTokenId, tokenHolders.labelTokens() );
        txState.labelDoCreateForName( labelName, internal, id );
        return id;
    }

    @Override
    public int relationshipTypeCreateForName( String relationshipTypeName, boolean internal ) throws KernelException
    {
        ktx.assertOpen();
        TransactionState txState = ktx.txState();
        int id = reserveTokenId( commandCreationContext::reserveRelationshipTypeTokenId, tokenHolders.relationshipTypeTokens() );
        txState.relationshipTypeDoCreateForName( relationshipTypeName, internal, id );
        return id;
    }

    @Override
    public int propertyKeyCreateForName( String propertyKeyName, boolean internal ) throws KernelException
    {
        ktx.assertOpen();
        TransactionState txState = ktx.txState();
        int id = reserveTokenId( commandCreationContext::reservePropertyKeyTokenId, tokenHolders.propertyKeyTokens() );
        txState.propertyKeyDoCreateForName( propertyKeyName, internal, id );
        return id;
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws KernelException
    {
        return getOrCreateForName( tokenHolders.propertyKeyTokens(), PrivilegeAction.CREATE_PROPERTYKEY, propertyKeyName );
    }

    @Override
    public void propertyKeyGetOrCreateForNames( String[] propertyKeys, int[] ids ) throws KernelException
    {
        getOrCreateForNames( tokenHolders.propertyKeyTokens(), PrivilegeAction.CREATE_PROPERTYKEY, propertyKeys, ids );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws KernelException
    {
        return getOrCreateForName( tokenHolders.relationshipTypeTokens(), PrivilegeAction.CREATE_RELTYPE, relationshipTypeName );
    }

    @Override
    public void relationshipTypeGetOrCreateForNames( String[] relationshipTypes, int[] ids ) throws KernelException
    {
        getOrCreateForNames( tokenHolders.relationshipTypeTokens(), PrivilegeAction.CREATE_RELTYPE, relationshipTypes, ids );
    }

    @Override
    public String nodeLabelName( int labelId ) throws LabelNotFoundKernelException
    {
        ktx.assertOpen();
        try
        {
            return tokenHolders.labelTokens().getTokenById( labelId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( labelId, e );
        }
    }

    @Override
    public int nodeLabel( String name )
    {
        ktx.assertOpen();
        return tokenHolders.labelTokens().getIdByName( name );
    }

    @Override
    public int relationshipType( String name )
    {
        ktx.assertOpen();
        return tokenHolders.relationshipTypeTokens().getIdByName( name );
    }

    @Override
    public String relationshipTypeName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        ktx.assertOpen();
        try
        {
            return tokenHolders.relationshipTypeTokens().getTokenById( relationshipTypeId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new RelationshipTypeIdNotFoundKernelException( relationshipTypeId, e );
        }
    }

    @Override
    public int propertyKey( String name )
    {
        ktx.assertOpen();
        return tokenHolders.propertyKeyTokens().getIdByName( name );
    }

    @Override
    public String propertyKeyName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        ktx.assertOpen();
        try
        {
            return tokenHolders.propertyKeyTokens().getTokenById( propertyKeyId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyIdNotFoundKernelException( propertyKeyId, e );
        }
    }

    @Override
    public Iterator<NamedToken> labelsGetAllTokens()
    {
        ktx.assertOpen();
        AccessMode mode = ktx.securityContext().mode();
        return Iterators.stream( tokenHolders.labelTokens().getAllTokens().iterator() )
                .filter( label -> mode.allowsTraverseNode( label.id() ) )
                .iterator();
    }

    @Override
    public Iterator<NamedToken> propertyKeyGetAllTokens()
    {
        ktx.assertOpen();
        AccessMode mode = ktx.securityContext().mode();
        return Iterators.stream( tokenHolders.propertyKeyTokens().getAllTokens().iterator() )
                .filter( propKey -> mode.allowsSeePropertyKeyToken( propKey.id() ) )
                .iterator();
    }

    @Override
    public Iterator<NamedToken> relationshipTypesGetAllTokens()
    {
        ktx.assertOpen();
        AccessMode mode = ktx.securityContext().mode();
        return Iterators.stream( tokenHolders.relationshipTypeTokens().getAllTokens().iterator() )
                .filter( relType -> mode.allowsTraverseRelType( relType.id() ) )
                .iterator();
    }

    @Override
    public int labelCount()
    {
        ktx.assertOpen();
        return store.labelCount();
    }

    @Override
    public int propertyKeyCount()
    {
        ktx.assertOpen();
        return store.propertyKeyCount();
    }

    @Override
    public int relationshipTypeCount()
    {
        ktx.assertOpen();
        return store.relationshipTypeCount();
    }

    private int getOrCreateForName( TokenHolder tokens, PrivilegeAction action, String name ) throws KernelException
    {
        ktx.assertOpen();
        int id = tokens.getIdByName( checkValidTokenName( name ) );
        if ( id != NO_TOKEN )
        {
            return id;
        }
        ktx.assertAllowsTokenCreates( action );
        return tokens.getOrCreateId( name );
    }

    private void getOrCreateForNames( TokenHolder tokenHolder, PrivilegeAction action, String[] names, int[] ids ) throws KernelException
    {
        ktx.assertOpen();
        assertSameLength( names, ids );
        for ( int i = 0; i < names.length; i++ )
        {
            ids[i] = tokenHolder.getIdByName( checkValidTokenName( names[i] ) );
            if ( ids[i] == NO_TOKEN )
            {
                ktx.assertAllowsTokenCreates( action );
                tokenHolder.getOrCreateIds( names, ids );
                return;
            }
        }
    }

    private static void assertSameLength( String[] names, int[] ids )
    {
        if ( names.length != ids.length )
        {
            throw new IllegalArgumentException( "Name and id arrays have different length." );
        }
    }

    private static int reserveTokenId( IntSupplier generator, TokenHolder holder ) throws KernelException
    {
        try
        {
            int id;
            do
            {
                id = generator.getAsInt();
            }
            while ( holder.hasToken( id ) ); // Retry if id is already taken.
            return id;
        }
        catch ( IdCapacityExceededException e )
        {
            throw new TokenCapacityExceededKernelException( e, holder.getTokenType() );
        }
    }

    @Override
    public String labelGetName( int labelId )
    {
        ktx.assertOpen();
        return tokenHolders.labelGetName( labelId );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId )
    {
        ktx.assertOpen();
        return tokenHolders.relationshipTypeGetName( relationshipTypeId );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        ktx.assertOpen();
        return tokenHolders.propertyKeyGetName( propertyKeyId );
    }
}
