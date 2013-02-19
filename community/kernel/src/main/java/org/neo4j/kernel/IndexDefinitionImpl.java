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

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.util.Collection;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.StatementContext;

class IndexDefinitionImpl implements IndexDefinition
{
    private final Label label;
    private final Collection<String> propertyKey;
    private final ThreadToStatementContextBridge ctxProvider;

    public IndexDefinitionImpl( ThreadToStatementContextBridge ctxProvider, Label label, String propertyKey )
    {
        this.ctxProvider = ctxProvider;
        this.label = label;
        this.propertyKey = asList( propertyKey );
    }

    @Override
    public Label getLabel()
    {
        return label;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return propertyKey;
    }

    @Override
    public void drop()
    {
        StatementContext context = ctxProvider.getCtxForWriting();
        try
        {
            context.dropIndexRule( context.getLabelId( label.name() ), context.getPropertyKeyId( single( propertyKey ) ) );
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "Dropping " + this + " should not violate any constraint" );
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "Label " + label.name() + " should exist here" );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "Property " + propertyKey + " should exist here" );
        }
    }
    
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + label.name().hashCode();
        result = prime * result + propertyKey.hashCode();
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
        IndexDefinitionImpl other = (IndexDefinitionImpl) obj;
        if ( !label.name().equals( other.label.name() ) )
            return false;
        if ( !propertyKey.equals( other.propertyKey ) )
            return false;
        return true;
    }
}
