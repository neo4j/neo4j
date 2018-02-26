/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.storageengine.api.StoreReadLayer;

public class KernelToken implements Token
{
    private final StoreReadLayer store;
    private final KernelTransactionImplementation ktx;

    public KernelToken( StoreReadLayer store, KernelTransactionImplementation ktx )
    {
        this.store = store;
        this.ktx = ktx;
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        return store.labelGetOrCreateForName( checkValidTokenName( labelName ) );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        return store.propertyKeyGetOrCreateForName( checkValidTokenName( propertyKeyName ) );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        ktx.assertOpen();
        return store.relationshipTypeGetOrCreateForName( checkValidTokenName( relationshipTypeName ) );
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
        return store.labelsGetAllTokens();
    }

    @Override
    public Iterator<NamedToken> propertyKeyGetAllTokens()
    {
        ktx.assertOpen();
        AccessMode mode = ktx.securityContext().mode();
        return Iterators.stream( store.propertyKeyGetAllTokens() ).
                filter( propKey -> mode.allowsPropertyReads( propKey.id() ) ).iterator();
    }

    @Override
    public Iterator<NamedToken> relationshipTypesGetAllTokens()
    {
        ktx.assertOpen();
        return store.relationshipTypeGetAllTokens();
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
