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

import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.api.TokenNameLookup;

public abstract class PropertyConstraint
{
    public interface ChangeVisitor
    {
        void visitAddedUniquePropertyConstraint( UniquenessConstraint constraint );

        void visitRemovedUniquePropertyConstraint( UniquenessConstraint constraint );

        void visitAddedMandatoryPropertyConstraint( MandatoryPropertyConstraint constraint );

        void visitRemovedMandatoryPropertyConstraint( MandatoryPropertyConstraint constraint );
    }

    private final int labelId;
    private final int propertyKeyId;

    public PropertyConstraint( int labelId, int propertyKeyId )
    {
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
    }

    public abstract void added( ChangeVisitor visitor );

    public abstract void removed( ChangeVisitor visitor );

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj != null && getClass() == obj.getClass() )
        {
            PropertyConstraint that = (PropertyConstraint) obj;
            return this.equals( type(), that.labelId, that.propertyKeyId );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = labelId;
        result = 31 * result + propertyKeyId;
        return result;
    }

    public int label()
    {
        return labelId;
    }

    public int propertyKeyId()
    {
        return propertyKeyId;
    }

    public boolean equals( ConstraintType type, int labelId, int propertyKeyId )
    {
        return this.labelId == labelId && this.propertyKeyId == propertyKeyId && type() == type;
    }

    @Override
    public String toString()
    {
        return String.format( "CONSTRAINT ON ( n:label[%s] ) ASSERT n.property[%s] IS %s",
                labelId, propertyKeyId, constraintString() );
    }

    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        String labelName = tokenNameLookup.labelGetName( labelId );
        String boundIdentifier = labelName.toLowerCase();
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s.%s IS %s", boundIdentifier, labelName,
                boundIdentifier, tokenNameLookup.propertyKeyGetName( propertyKeyId ), constraintString() );
    }

    abstract String constraintString();

    public abstract ConstraintType type();
}
