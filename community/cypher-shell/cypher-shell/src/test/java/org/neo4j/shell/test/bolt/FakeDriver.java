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
package org.neo4j.shell.test.bolt;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.TypeSystem;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class FakeDriver implements Driver
{
    @Override
    public boolean isEncrypted()
    {
        return false;
    }

    @Override
    public Session session()
    {
        return new FakeSession();
    }

    @Override
    public Session session( SessionConfig sessionConfig )
    {
        return new FakeSession();
    }

    @Override
    public void close() throws Neo4jException
    {
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        return null;
    }

    @Override
    public Metrics metrics()
    {
        return null;
    }

    @Override
    public boolean isMetricsEnabled()
    {
        return false;
    }

    @Override
    public RxSession rxSession()
    {
        return null;
    }

    @Override
    public RxSession rxSession( SessionConfig sessionConfig )
    {
        return null;
    }

    @Override
    public AsyncSession asyncSession()
    {
        return null;
    }

    @Override
    public AsyncSession asyncSession( SessionConfig sessionConfig )
    {
        return null;
    }

    @Override
    public TypeSystem defaultTypeSystem()
    {
        return null;
    }

    @Override
    public void verifyConnectivity()
    {
    }

    @Override
    public CompletionStage<Void> verifyConnectivityAsync()
    {
        return null;
    }

    @Override
    public boolean supportsMultiDb()
    {
        return true;
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDbAsync()
    {
        return completedFuture( true );
    }
}
