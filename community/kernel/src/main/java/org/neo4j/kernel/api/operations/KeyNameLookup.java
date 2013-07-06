/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.operations;

import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;

public class KeyNameLookup
{
    private final KeyReadOperations keyReadOperations;
    private final StatementState state;

    public KeyNameLookup( StatementState state, KeyReadOperations context )
    {
        this.state = state;
        this.keyReadOperations = context;
    }

    /**
     * Returns the label name for the given label id. In case of downstream failure, returns label[id].
     */
    public String getLabelName( long labelId )
    {
        try
        {
            return keyReadOperations.labelGetName( state, labelId );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return "[" + labelId + "]";
        }
    }

    /**
     * Returns the name of a property given its property key id. In case of downstream failure, returns property[id].
     */
    public String getPropertyKeyName( long propertyId )
    {
        try
        {
            return keyReadOperations.propertyKeyGetName( state, propertyId );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return "[" + propertyId + "]";
        }
    }
}
