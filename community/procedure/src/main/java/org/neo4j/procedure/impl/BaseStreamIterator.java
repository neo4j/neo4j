/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.procedure.impl;

import java.util.Iterator;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

/**
 * Base iterator class used extended by generated code to map from procedure streams
 */
@SuppressWarnings( "WeakerAccess" )
public abstract class BaseStreamIterator implements RawIterator<AnyValue[],ProcedureException>, Resource
{
    private final Iterator<?> out;
    private Stream<?> stream;
    private final ResourceTracker resourceTracker;
    private final ProcedureSignature signature;

    public BaseStreamIterator( Stream<?> stream, ResourceTracker resourceTracker,
            ProcedureSignature signature )
    {
        this.out = stream.iterator();
        this.stream = stream;
        this.resourceTracker = resourceTracker;
        this.signature = signature;
        resourceTracker.registerCloseableResource( stream );
    }

    public abstract AnyValue[] map( Object in );

    @Override
    public boolean hasNext() throws ProcedureException
    {
        try
        {
            boolean hasNext = out.hasNext();
            if ( !hasNext )
            {
                close();
            }
            return hasNext;
        }
        catch ( Throwable throwable )
        {
            throw closeAndCreateProcedureException( throwable );
        }
    }

    @Override
    public AnyValue[] next() throws ProcedureException
    {
        try
        {
            Object record = out.next();
            return map( record );
        }
        catch ( Throwable throwable )
        {
            throw closeAndCreateProcedureException( throwable );
        }
    }

    @Override
    public void close()
    {
        if ( stream != null )
        {
            // Make sure we reset closeableResource before doing anything which may throw an exception that may
            // result in a recursive call to this close-method
            AutoCloseable resourceToClose = stream;
            stream = null;
            IOUtils.close( ResourceCloseFailureException::new,
                    () -> resourceTracker.unregisterCloseableResource( resourceToClose ),
                    resourceToClose );
        }
    }

    private ProcedureException closeAndCreateProcedureException( Throwable t )
    {
        ProcedureException procedureException = newProcedureException( t );

        try
        {
            close();
        }
        catch ( Exception exceptionDuringClose )
        {
            try
            {
                procedureException.addSuppressed( exceptionDuringClose );
            }
            catch ( Throwable ignore )
            {
            }
        }
        return procedureException;
    }

    private ProcedureException newProcedureException( Throwable throwable )
    {
        if ( throwable instanceof Status.HasStatus )
        {
            return new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                    throwable.getMessage() );
        }
        else
        {
            Throwable cause = getRootCause( throwable );
            return new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                    "Failed to invoke procedure `%s`: %s", signature.name(),
                    "Caused by: " + (cause != null ? cause : throwable) );
        }
    }
}
