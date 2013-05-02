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
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.KernelException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;

public class PropertyUniqueConstraintDefinition extends AbstractConstraintDefinition implements UniquenessConstraintDefinition
{
    private final UniquenessConstraint constraint;

    PropertyUniqueConstraintDefinition( ThreadToStatementContextBridge ctxProvider, Label label,
            UniquenessConstraint constraint )
    {
        super( ctxProvider, label );
        this.constraint = constraint;
    }

    @Override
    public Type getConstraintType()
    {
        return Type.UNIQUENESS;
    }

    @Override
    protected void doDrop( StatementContext context ) throws KernelException
    {
        context.dropConstraint( constraint );
    }

    @Override
    public Iterable<String> getPropertyKey()
    {
        return singletonList( getPropertyKeyName() );
    }
    
    private String getPropertyKeyName()
    {
        StatementContext context = ctxProvider.getCtxForReading();
        try
        {
            return context.getPropertyKeyName( constraint.property() );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "Missing property key " + constraint.property() );
        }
        finally
        {
            context.close();
        }
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
        int result = 1;
        result = prime * result + ((constraint == null) ? 0 : constraint.hashCode());
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
        PropertyUniqueConstraintDefinition other = (PropertyUniqueConstraintDefinition) obj;
        if ( constraint == null )
        {
            if ( other.constraint != null )
                return false;
        }
        else if ( !constraint.equals( other.constraint ) )
            return false;
        return true;
    }
}
