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
package org.neo4j.kernel;

import static java.util.Collections.singletonList;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;

public class PropertyUniqueConstraintDefinition extends BaseConstraintDefinition implements UniquenessConstraintDefinition
{
    private final String propertyKey;

    public PropertyUniqueConstraintDefinition( InternalSchemaActions actions, Label label, String propertyKey )
    {
        super( actions, label );
        this.propertyKey = propertyKey;
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.UNIQUENESS;
    }
    
    @Override
    public void drop()
    {
        actions.dropPropertyUniquenessConstraint( label, propertyKey );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return singletonList( propertyKey );
    }

    @Override
    public UniquenessConstraintDefinition asUniquenessConstraint()
    {
        return this;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((propertyKey == null) ? 0 : propertyKey.hashCode());
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( !super.equals( obj ) )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        PropertyUniqueConstraintDefinition other = (PropertyUniqueConstraintDefinition) obj;
        if ( propertyKey == null )
        {
            if ( other.propertyKey != null )
                return false;
        }
        else if ( !propertyKey.equals( other.propertyKey ) )
            return false;
        return true;
    }
}
