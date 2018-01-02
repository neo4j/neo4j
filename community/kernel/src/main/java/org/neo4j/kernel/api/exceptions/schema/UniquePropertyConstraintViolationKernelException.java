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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.kernel.api.TokenNameLookup;

import static java.lang.String.format;

public class UniquePropertyConstraintViolationKernelException extends ConstraintViolationKernelException
{
    private final int labelId;
    private final int propertyKeyId;
    private final Object value;
    private final long existingNodeId;

    public UniquePropertyConstraintViolationKernelException( int labelId, int propertyKeyId, Object value,
            long existingNodeId )
    {
        super( "Node %d already exists with label %d and property %d=%s", existingNodeId, labelId, propertyKeyId, value );
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
        this.value = value;
        this.existingNodeId = existingNodeId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( "Node %d already exists with label %s and property \"%s\"=[%s]", existingNodeId,
                tokenNameLookup.labelGetName( labelId ),
                tokenNameLookup.propertyKeyGetName( propertyKeyId ),
                value );
    }

    public int labelId()
    {
        return labelId;
    }

    public int propertyKeyId()
    {
        return propertyKeyId;
    }

    public Object propertyValue()
    {
        return value;
    }
}
