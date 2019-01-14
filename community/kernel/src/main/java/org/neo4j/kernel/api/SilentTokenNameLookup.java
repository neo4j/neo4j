/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;

/**
 * Instances allow looking up ids back to their names.
 *
 */
public final class SilentTokenNameLookup implements TokenNameLookup
{
    private final TokenRead tokenRead;

    public SilentTokenNameLookup( TokenRead tokenRead )
    {
        this.tokenRead = tokenRead;
    }

    /**
     * Returns the a label name given its id. In case of downstream failure, returns [labelId].
     */
    @Override
    public String labelGetName( int labelId )
    {
        try
        {
            return tokenRead.nodeLabelName( labelId );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return "[" + labelId + "]";
        }
    }

    /**
     * Returns the name of a relationship type given its id. In case of downstream failure, returns [relationshipTypeId].
     */
    @Override
    public String relationshipTypeGetName( int relTypeId )
    {
        try
        {
            return tokenRead.relationshipTypeName( relTypeId );
        }
        catch ( KernelException e )
        {
            return "[" + relTypeId + "]";
        }
    }

    /**
     * Returns the name of a property given its id. In case of downstream failure, returns [propertyId].
     */
    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        try
        {
            return tokenRead.propertyKeyName( propertyKeyId );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            return "[" + propertyKeyId + "]";
        }
    }
}
