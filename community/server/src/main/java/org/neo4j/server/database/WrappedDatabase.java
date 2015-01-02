/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.database;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;

public class WrappedDatabase extends CommunityDatabase
{
    private final AbstractGraphDatabase db;

    public WrappedDatabase( AbstractGraphDatabase db )
    {
        this( db, new ServerConfigurator( db ) );
    }

    public WrappedDatabase( AbstractGraphDatabase db, Configurator configurator )
    {
        super( configurator, getLogging( db ) );
        this.db = db;
        try
        {
            start();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }
    }

    @Override
    protected AbstractGraphDatabase createDb()
    {
        return db;
    }

    @Override
    public void stop() throws Throwable
    {
        // No-op
    }

    @Override
    public Logging getLogging()
    {
        return getLogging( db );
    }

    private static Logging getLogging( AbstractGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( Logging.class );
    }
}
