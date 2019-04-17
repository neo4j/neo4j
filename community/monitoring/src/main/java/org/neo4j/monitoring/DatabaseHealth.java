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
package org.neo4j.monitoring;

import java.util.Objects;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

public class DatabaseHealth extends LifecycleAdapter implements Health
{
    private static final String panicMessage = "The database has encountered a critical error, " +
            "and needs to be restarted. Please see database logs for more details.";
    private static final Class<?>[] CRITICAL_EXCEPTIONS = new Class[]{OutOfMemoryError.class};

    private volatile boolean healthy = true;
    private final DatabasePanicEventGenerator panicEventGenerator;
    private final Log log;
    private volatile Throwable causeOfPanic;
    private final CompositeDatabaseHealth globalHealth;

    public DatabaseHealth( DatabasePanicEventGenerator panicEventGenerator, Log log )
    {
        this( panicEventGenerator, log, null );
    }

    public DatabaseHealth( DatabasePanicEventGenerator panicEventGenerator, Log log, CompositeDatabaseHealth globalHealth )
    {
        this.panicEventGenerator = panicEventGenerator;
        this.log = log;
        this.globalHealth = globalHealth;
    }

    @Override
    public void stop() throws Exception
    {
       if ( globalHealth != null )
       {
           globalHealth.removeDatabaseHealth( this );
       }
    }

    /**
     * Asserts that the database is in good health. If that is not the case then the cause of the
     * unhealthy state is wrapped in an exception of the given type, i.e. the panic disguise.
     *
     * @param panicDisguise the cause of the unhealthy state wrapped in an exception of this type.
     * @throws EXCEPTION exception type to wrap cause in.
     */
    @Override
    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
    {
        if ( !healthy )
        {
            throw Exceptions.disguiseException( panicDisguise, panicMessage, causeOfPanic );
        }
    }

    @Override
    public synchronized void panic( Throwable cause )
    {
        if ( !healthy )
        {
            return;
        }

        Objects.requireNonNull( cause, "Must provide a non null cause for the database panic" );
        this.causeOfPanic = cause;
        this.healthy = false;
        log.error( "Database panic: " + panicMessage, cause );
        panicEventGenerator.panic();
    }

    @Override
    public boolean isHealthy()
    {
        return healthy;
    }

    @Override
    public synchronized boolean healed()
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

    @Override
    public Throwable cause()
    {
        return causeOfPanic;
    }

}
