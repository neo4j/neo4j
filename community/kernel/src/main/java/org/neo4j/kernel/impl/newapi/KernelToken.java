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

import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.TimeZoneNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.storageengine.api.StoreReadLayer;

public class KernelToken implements Token
{
    private final StoreReadLayer store;

    public KernelToken( StoreReadLayer store )
    {
        this.store = store;
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        return store.labelGetOrCreateForName( checkValidTokenName( labelName ) );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        return store.propertyKeyGetOrCreateForName( checkValidTokenName( propertyKeyName ) );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        return store.relationshipTypeGetOrCreateForName( checkValidTokenName( relationshipTypeName ) );
    }

    @Override
    public String nodeLabelName( int labelId ) throws LabelNotFoundKernelException
    {
        return store.labelGetName( labelId );
    }

    @Override
    public int nodeLabel( String name )
    {
        return store.labelGetForName( name );
    }

    @Override
    public String timeZoneName( int timeZoneId ) throws TimeZoneNotFoundKernelException
    {
        return store.timeZoneGetName( timeZoneId );
    }

    @Override
    public int timeZone( String name )
    {
        return store.timeZoneGetForName( name );
    }

    @Override
    public int relationshipType( String name )
    {
        return store.relationshipTypeGetForName( name );
    }

    @Override
    public String relationshipTypeName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        return store.relationshipTypeGetName( relationshipTypeId );
    }

    @Override
    public int propertyKey( String name )
    {
        return store.propertyKeyGetForName( name );
    }

    @Override
    public String propertyKeyName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        return store.propertyKeyGetName( propertyKeyId );
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
