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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;

public abstract class BaseConstraintDefinition implements ConstraintDefinition
{
    protected final InternalSchemaActions actions;
    protected final Label label;

    public BaseConstraintDefinition( InternalSchemaActions actions, Label label )
    {
        this.actions = actions;
        this.label = label;
    }

    @Override
    public Label getLabel()
    {
        return label;
    }

    @Override
    public boolean isConstraintType( ConstraintType type )
    {
        return getConstraintType().equals( type );
    }

    @Override
    public UniquenessConstraintDefinition asUniquenessConstraint()
    {
        throw new UnsupportedOperationException( this + " is of type " + getClass().getSimpleName() );
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        BaseConstraintDefinition other = (BaseConstraintDefinition) obj;
        if ( label == null )
        {
            if ( other.label != null )
                return false;
        }
        else if ( !label.name().equals( other.label.name() ) )
            return false;
        return true;
    }
}
