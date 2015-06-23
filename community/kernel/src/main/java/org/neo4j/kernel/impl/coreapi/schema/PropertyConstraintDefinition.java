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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;

import static java.util.Collections.singletonList;

public class PropertyConstraintDefinition implements ConstraintDefinition
{
    private final InternalSchemaActions actions;
    private final Label label;
    private final String propertyKey;
    private final ConstraintType type;

    public PropertyConstraintDefinition( InternalSchemaActions actions, Label label, String propertyKey,
            ConstraintType type )
    {
        this.actions = actions;
        this.label = label;
        this.propertyKey = propertyKey;
        this.type = type;
    }

    @Override
    public void drop()
    {
        assertInUnterminatedTransaction();
        actions.dropPropertyUniquenessConstraint( label, propertyKey );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        assertInUnterminatedTransaction();
        return singletonList( propertyKey );
    }

    @Override
    public Label getLabel()
    {
        assertInUnterminatedTransaction();
        return label;
    }

    @Override
    public ConstraintType getConstraintType()
    {
        assertInUnterminatedTransaction();
        return type;
    }

    @Override
    public boolean isConstraintType( ConstraintType type )
    {
        assertInUnterminatedTransaction();
        return getConstraintType().equals( type );
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

        PropertyConstraintDefinition that = (PropertyConstraintDefinition) o;

        if ( type != that.type )
        {
            return false;
        }
        if ( !actions.equals( that.actions ) )
        {
            return false;
        }
        if ( !label.equals( that.label ) )
        {
            return false;
        }
        if ( !propertyKey.equals( that.propertyKey ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + label.name().hashCode();
        result = 31 * result + propertyKey.hashCode();

        return result;
    }

    @Override
    public String toString()
    {
        // using label name as a good identifier name
        return String.format( "%s.%s IS %s", label.name().toLowerCase(), propertyKey, constraintString() );
    }

    private String constraintString()
    {
        switch ( type )
        {
        case UNIQUENESS:
            return "UNIQUE";
        case MANDATORY:
            return "NOT NULL";
        default:
            throw new UnsupportedOperationException( "Unknown ConstraintType: " + type );
        }
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
