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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.util.VisibleForTesting;

/**
 * Composite database health service that makes decisions about its health based on multiple underlying database specific health services.
 * Any panics, asserts and other checks are redistributed to all underlying health services.
 */
public class CompositeDatabaseHealth implements Health
{
    private final CopyOnWriteArrayList<DatabaseHealth> healths;
    private volatile Throwable rootCauseOfPanic;

    public CompositeDatabaseHealth()
    {
        this.healths = new CopyOnWriteArrayList<>();
    }

    @VisibleForTesting
    CompositeDatabaseHealth( Collection<DatabaseHealth> healths )
    {
        this.healths = new CopyOnWriteArrayList<>( healths );
    }

    public DatabaseHealth createDatabaseHealth( DatabasePanicEventGenerator dbpe, Log log )
    {
        DatabaseHealth databaseHealth = new DatabaseHealth( dbpe, log );
        healths.add( databaseHealth );
        return databaseHealth;
    }

    void removeDatabaseHealth( DatabaseHealth health )
    {
        healths.remove( health );
    }

    @Override
    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
    {
        for ( DatabaseHealth healthService : healths )
        {
            healthService.assertHealthy( panicDisguise );
        }
    }

    @Override
    public synchronized void panic( Throwable cause )
    {
        if ( rootCauseOfPanic != null )
        {
            return;
        }

        Objects.requireNonNull( cause, "Must provide a non null cause for the panic" );
        rootCauseOfPanic = cause;
        for ( DatabaseHealth healthService : healths )
        {
            healthService.panic( cause );
        }
    }

    @Override
    public boolean isHealthy()
    {
        return healths.stream().allMatch( Health::isHealthy );
    }

    @Override
    public boolean healed()
    {
        return healths.stream().allMatch( Health::healed );
    }

    @Override
    public Throwable cause()
    {
        List<Throwable> exceptions = healths.stream().flatMap( h -> Optional.ofNullable( h.cause() ).stream() ).collect( Collectors.toList() );
        if ( exceptions.isEmpty() )
        {
            return null;
        }
        Throwable root = rootCauseOfPanic;
        Throwable identity = root != null ? root : new Exception( "Some of the databases have panicked!" );
        return exceptions.stream()
                .reduce( identity, ( acc, next ) ->
                {
                    acc.addSuppressed( next );
                    return acc;
                } );
    }
}
