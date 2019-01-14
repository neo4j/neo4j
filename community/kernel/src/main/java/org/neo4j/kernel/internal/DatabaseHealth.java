/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.internal;

import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.logging.Log;

public class DatabaseHealth
{
    private static final String panicMessage = "The database has encountered a critical error, " +
            "and needs to be restarted. Please see database logs for more details.";
    private static final Class<?>[] CRITICAL_EXCEPTIONS = new Class[]{OutOfMemoryError.class};

    private volatile boolean healthy = true;
    private final DatabasePanicEventGenerator dbpe;
    private final Log log;
    private Throwable causeOfPanic;

    public DatabaseHealth( DatabasePanicEventGenerator dbpe, Log log )
    {
        this.dbpe = dbpe;
        this.log = log;
    }

    /**
     * Asserts that the database is in good health. If that is not the case then the cause of the
     * unhealthy state is wrapped in an exception of the given type, i.e. the panic disguise.
     *
     * @param panicDisguise the cause of the unhealthy state wrapped in an exception of this type.
     * @throws EXCEPTION exception type to wrap cause in.
     */
    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
    {
        if ( !healthy )
        {
            EXCEPTION exception;
            try
            {
                try
                {
                    exception = panicDisguise.getConstructor( String.class, Throwable.class )
                            .newInstance( panicMessage, causeOfPanic );
                }
                catch ( NoSuchMethodException e )
                {
                    exception = panicDisguise.getConstructor( String.class ).newInstance( panicMessage );
                    try
                    {
                        exception.initCause( causeOfPanic );
                    }
                    catch ( IllegalStateException ignored )
                    {
                    }
                }
            }
            catch ( Exception e )
            {
                throw new Error( panicMessage + ". An exception of type " + panicDisguise.getName() +
                        " was requested to be thrown but that proved impossible", e );
            }
            throw exception;
        }
    }

    public void panic( Throwable cause )
    {
        if ( !healthy )
        {
            return;
        }

        if ( cause == null )
        {
            throw new IllegalArgumentException( "Must provide a cause for the database panic" );
        }
        this.causeOfPanic = cause;
        this.healthy = false;
        log.error( "Database panic: " + panicMessage, cause );
        dbpe.generateEvent( ErrorState.TX_MANAGER_NOT_OK, causeOfPanic );
    }

    public boolean isHealthy()
    {
        return healthy;
    }

    public boolean healed()
    {
        if ( hasCriticalFailure() )
        {
            log.error( "Database encountered a critical error and can't be healed. Restart required." );
            return false;
        }
        else
        {
            healthy = true;
            causeOfPanic = null;
            log.info( "Database health set to OK" );
            return true;
        }
    }

    private boolean hasCriticalFailure()
    {
        return !isHealthy() && Exceptions.contains( causeOfPanic, CRITICAL_EXCEPTIONS );
    }

    public Throwable cause()
    {
        return causeOfPanic;
    }
}
