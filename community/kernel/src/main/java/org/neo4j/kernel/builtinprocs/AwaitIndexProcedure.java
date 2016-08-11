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

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;

import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;

public class AwaitIndexProcedure
{
    private ReadOperations operations;

    public AwaitIndexProcedure( KernelTransaction tx )
    {
        operations = tx.acquireStatement().readOperations();
    }

    public void execute( String labelName, String propertyKeyName ) throws ProcedureException
    {
        int labelId = getLabelId( labelName );
        int propertyKeyId = getPropertyKeyId( propertyKeyName );

        if ( ONLINE != getState( labelName, propertyKeyName, labelId, propertyKeyId ) )
        {
            throw new ProcedureException( Status.General.UnknownError, "Index not online" );
        }
    }

    private InternalIndexState getState( String labelName, String propertyKeyName, int labelId, int propertyKeyId )
            throws ProcedureException
    {
        try
        {
            IndexDescriptor index = operations.indexGetForLabelAndPropertyKey( labelId, propertyKeyId );
            return operations.indexGetState( index );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e,
                    "No index on :%s(%s)", labelName, propertyKeyName );
        }
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

    private int getLabelId( String labelName ) throws ProcedureException
    {
        int labelId = operations.labelGetForName( labelName );
        if ( labelId == ReadOperations.NO_SUCH_LABEL )
        {
            throw new ProcedureException( Status.Schema.LabelAccessFailed, "No such label %s", labelName );
        }
        return labelId;
    }
}
