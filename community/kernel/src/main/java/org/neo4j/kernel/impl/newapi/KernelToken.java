/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.storageengine.api.StorageReader;

public class KernelToken implements Token
{
    private final StorageReader store;
    private final KernelTransactionImplementation ktx;

    public KernelToken( StorageReader store, KernelTransactionImplementation ktx )
    {
        this.store = store;
        this.ktx = ktx;
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        ktx.assertOpen();
        int labelId = store.labelGetForName( checkValidTokenName( labelName ) );
        if ( labelId != TokenHolder.NO_ID )
        {
            return labelId;
        }
        ktx.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
        return store.labelGetOrCreateForName( labelName );
    }

    @Override
    public void labelGetOrCreateForNames( String[] labelNames, int[] labelIds )
            throws IllegalTokenNameException, TooManyLabelsException
    {
        ktx.assertOpen();
        assertSameLength( labelNames, labelIds );
        for ( int i = 0; i < labelNames.length; i++ )
        {
            labelIds[i] = store.labelGetForName( checkValidTokenName( labelNames[i] ) );
            if ( labelIds[i] == TokenHolder.NO_ID )
            {
                ktx.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
                store.labelGetOrCreateForNames( labelNames, labelIds );
                return;
            }
        }
    }

    private void assertSameLength( String[] names, int[] ids )
    {
        if ( names.length != ids.length )
        {
            throw new IllegalArgumentException( "Name and id arrays have different length." );
        }
    }

    @Override
    public void labelCreateForName( String labelName, int id ) throws IllegalTokenNameException, TooManyLabelsException
    {
        ktx.assertOpen();
        ktx.txState().labelDoCreateForName( labelName, id );
    }

    @Override
    public void relationshipTypeCreateForName( String relationshipTypeName, int id ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        ktx.txState().relationshipTypeDoCreateForName( relationshipTypeName, id );
    }

    @Override
    public void propertyKeyCreateForName( String propertyKeyName, int id ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        ktx.txState().propertyKeyDoCreateForName( propertyKeyName, id );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        int propertyId = store.propertyKeyGetForName( checkValidTokenName( propertyKeyName ) );
        if ( propertyId != TokenHolder.NO_ID )
        {
            return propertyId;
        }
        ktx.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
        return store.propertyKeyGetOrCreateForName( propertyKeyName );
    }

    @Override
    public void propertyKeyGetOrCreateForNames( String[] propertyKeys, int[] ids ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        assertSameLength( propertyKeys, ids );
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            ids[i] = store.propertyKeyGetForName( checkValidTokenName( propertyKeys[i] ) );
            if ( ids[i] == TokenHolder.NO_ID )
            {
                ktx.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
                store.propertyKeyGetOrCreateForNames( propertyKeys, ids );
                return;
            }
        }
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        int typeId = store.relationshipTypeGetForName( checkValidTokenName( relationshipTypeName ) );
        if ( typeId != TokenHolder.NO_ID )
        {
            return typeId;
        }
        ktx.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
        return store.relationshipTypeGetOrCreateForName( relationshipTypeName );
    }

    @Override
    public void relationshipTypeGetOrCreateForNames( String[] relationshipTypes, int[] ids )
            throws IllegalTokenNameException
    {
        ktx.assertOpen();
        assertSameLength( relationshipTypes, ids );
        for ( int i = 0; i < relationshipTypes.length; i++ )
        {
            ids[i] = store.relationshipTypeGetForName( checkValidTokenName( relationshipTypes[i] ) );
            if ( ids[i] == TokenHolder.NO_ID )
            {
                ktx.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
                store.relationshipTypeGetOrCreateForNames( relationshipTypes, ids );
                return;
            }
        }
    }

    @Override
    public String nodeLabelName( int labelId ) throws LabelNotFoundKernelException
    {
        ktx.assertOpen();
        return store.labelGetName( labelId );
    }

    @Override
    public int nodeLabel( String name )
    {
        ktx.assertOpen();
        return store.labelGetForName( name );
    }

    @Override
    public int relationshipType( String name )
    {
        ktx.assertOpen();
        return store.relationshipTypeGetForName( name );
    }

    @Override
    public String relationshipTypeName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        ktx.assertOpen();
        return store.relationshipTypeGetName( relationshipTypeId );
    }

    @Override
    public int propertyKey( String name )
    {
        ktx.assertOpen();
        return store.propertyKeyGetForName( name );
    }

    @Override
    public String propertyKeyName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        ktx.assertOpen();
        return store.propertyKeyGetName( propertyKeyId );
    }

    @Override
    public Iterator<NamedToken> labelsGetAllTokens()
    {
        ktx.assertOpen();
        return Iterators.map( token -> new NamedToken( token.name(), token.id() ), store.labelsGetAllTokens());
    }

    @Override
    public Iterator<NamedToken> propertyKeyGetAllTokens()
    {
        ktx.assertOpen();
        AccessMode mode = ktx.securityContext().mode();
        return Iterators.stream( store.propertyKeyGetAllTokens() )
                .filter( propKey -> mode.allowsPropertyReads( propKey.id() ) )
                .map( token -> new NamedToken( token.name(), token.id() ) )
                .iterator();
    }

    @Override
    public Iterator<NamedToken> relationshipTypesGetAllTokens()
    {
        ktx.assertOpen();
        return Iterators.map( token -> new NamedToken( token.name(), token.id() ), store.relationshipTypeGetAllTokens());
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

    private String checkValidTokenName( String name ) throws IllegalTokenNameException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new IllegalTokenNameException( name );
        }
        return name;
    }
}
