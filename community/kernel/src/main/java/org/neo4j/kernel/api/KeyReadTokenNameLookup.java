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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

/**
 * Instances allow looking up ids back to their names.
 */
public final class KeyReadTokenNameLookup implements TokenNameLookup
{
    private final KeyReadOperations read;

    public KeyReadTokenNameLookup( KeyReadOperations read )
    {
        this.read = read;
    }

    /**
     * Returns the label name for the given label id. In case of downstream failure, returns label[id].
     */
    @Override
    public String labelGetName( int labelId )
    {
        try
        {
            return read.labelGetName( null, labelId );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return "[" + labelId + "]";
        }
    }

    @Override
    public String relationshipTypeGetName( int relTypeId )
    {
        try
        {
            return read.relationshipTypeGetName( null, relTypeId );
        }
        catch ( RelationshipTypeIdNotFoundKernelException e )
        {
            return "[" + relTypeId + "]";
        }
    }

    /**
     * Returns the name of a property given its property key id. In case of downstream failure, returns property[id].
     */
    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        try
        {
            return read.propertyKeyGetName( null, propertyKeyId );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            return "[" + propertyKeyId + "]";
        }
    }
}
