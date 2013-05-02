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

import static java.lang.String.format;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.KernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;

abstract class AbstractConstraintDefinition implements ConstraintDefinition
{
    protected final ThreadToStatementContextBridge ctxProvider;
    protected final Label label;

    AbstractConstraintDefinition( ThreadToStatementContextBridge ctxProvider, Label label )
    {
        this.ctxProvider = ctxProvider;
        this.label = label;
    }

    @Override
    public Label getLabel()
    {
        return label;
    }

    @Override
    public final void drop()
    {
        StatementContext context = ctxProvider.getCtxForWriting();
        try
        {
            doDrop( context );
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new ConstraintViolationException( format(
                    "Unable to drop constraint on label `%s`.", label.name() ), e );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new ConstraintViolationException( format(
                    "Unable to drop constraint on label `%s`.", label.name() ), e );
        }
        catch ( KernelException e )
        {
            throw new ThisShouldNotHappenError( "Mattias",
                    format( "Unexpected exception when dropping constraint for label %s", label.name() ) );
        }
        finally
        {
            context.close();
        }
    }

    protected abstract void doDrop( StatementContext context ) throws KernelException;

    @Override
    public UniquenessConstraintDefinition asUniquenessConstraint()
    {
        throw new UnsupportedOperationException( this + " is of type " + getClass().getSimpleName() );
    }
}
