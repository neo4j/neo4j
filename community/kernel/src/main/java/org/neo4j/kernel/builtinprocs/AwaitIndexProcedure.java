/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.function.Predicates;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;

import static java.lang.String.format;

public class AwaitIndexProcedure implements AutoCloseable
{
    private final ReadOperations operations;
    private Statement statement;

    public AwaitIndexProcedure( KernelTransaction tx )
    {
        statement = tx.acquireStatement();
        operations = statement.readOperations();
    }

    public void execute( String labelName, String propertyKeyName, long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        int labelId = getLabelId( labelName );
        int propertyKeyId = getPropertyKeyId( propertyKeyName );
        String indexDescription = formatIndex( labelName, propertyKeyName );
        IndexDescriptor index = getIndex( labelId, propertyKeyId, indexDescription );
        waitUntilOnline( index, indexDescription, timeout, timeoutUnits );
    }

    private int getLabelId( String labelName ) throws ProcedureException
    {
        int labelId = operations.labelGetForName( labelName );
        if ( labelId == ReadOperations.NO_SUCH_LABEL )
        {
            throw new ProcedureException( Status.Schema.LabelAccessFailed, "No such label %s", labelName );
        }
        return labelId;
    }

    private int getPropertyKeyId( String propertyKeyName ) throws ProcedureException
    {
        int propertyKeyId = operations.propertyKeyGetForName( propertyKeyName );
        if ( propertyKeyId == ReadOperations.NO_SUCH_PROPERTY_KEY )
        {
            throw new ProcedureException( Status.Schema.PropertyKeyAccessFailed,
                    "No such property key %s", propertyKeyName );
        }
        return propertyKeyId;
    }

    private IndexDescriptor getIndex( int labelId, int propertyKeyId, String indexDescription ) throws
            ProcedureException
    {
        try
        {
            return operations.indexGetForLabelAndPropertyKey( labelId, propertyKeyId );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No index on %s", indexDescription );
        }
    }

    private void waitUntilOnline( IndexDescriptor index, String indexDescription, long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        try
        {
            Predicates.awaitEx( () -> isOnline( indexDescription, index ), timeout, timeoutUnits );
        }
        catch ( TimeoutException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureTimedOut,
                    "Index on %s did not come online within %s %s", indexDescription, timeout, timeoutUnits );
        }
        catch ( InterruptedException e )
        {
            throw new ProcedureException( Status.General.DatabaseUnavailable,
                    "Interrupted waiting for index on %s to come online", indexDescription );
        }
    }

    private boolean isOnline( String indexDescription, IndexDescriptor index ) throws ProcedureException
    {
        InternalIndexState state = getState( indexDescription, index );
        switch ( state )
        {
            case POPULATING:
                return false;
            case ONLINE:
                return true;
            case FAILED:
                throw new ProcedureException( Status.Schema.IndexCreationFailed,
                        "Index on %s is in failed state", indexDescription );
            default:
                throw new IllegalStateException( "Unknown index state " + state );
        }
    }

    private InternalIndexState getState( String indexDescription, IndexDescriptor index ) throws ProcedureException
    {
        try
        {
            return operations.indexGetState( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No index on %s", indexDescription );
        }
    }

    private String formatIndex( String labelName, String propertyKeyName )
    {
        return format( ":%s(%s)", labelName, propertyKeyName );
    }

    @Override
    public void close()
    {
        statement.close();
    }
}
