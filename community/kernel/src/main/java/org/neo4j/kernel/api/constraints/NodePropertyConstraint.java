/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.constraints;

import org.neo4j.kernel.api.TokenNameLookup;

public abstract class NodePropertyConstraint extends PropertyConstraint
{
    private final int labelId;

    public NodePropertyConstraint( int labelId, int propertyKeyId )
    {
        super( propertyKeyId );
        this.labelId = labelId;
    }

    public int label()
    {
        return labelId;
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        String labelName = tokenNameLookup.labelGetName( labelId );
        String boundIdentifier = labelName.toLowerCase();
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s.%s IS %s", boundIdentifier, labelName,
                boundIdentifier, tokenNameLookup.propertyKeyGetName( propertyKeyId ), constraintString() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        NodePropertyConstraint that = (NodePropertyConstraint) o;
        return propertyKeyId == that.propertyKeyId && labelId == that.labelId;

    }

    @Override
    public int hashCode()
    {
        return 31 * propertyKeyId + labelId;
    }

    @Override
    public String toString()
    {
        return String.format( "CONSTRAINT ON ( n:label[%s] ) ASSERT n.property[%s] IS %s",
                labelId, propertyKeyId, constraintString() );
    }
}
