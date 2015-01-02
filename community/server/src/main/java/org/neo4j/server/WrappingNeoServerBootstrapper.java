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
package org.neo4j.server;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;

/**
 * A bootstrapper for the Neo4j Server that takes an already instantiated
 * {@link org.neo4j.kernel.GraphDatabaseAPI}, and optional configuration, and launches a
 * server using that database.
 */
@Deprecated
public class WrappingNeoServerBootstrapper extends Bootstrapper
{
    private final GraphDatabaseAPI db;
    private final Configurator configurator;

    /**
     * Create an instance with default settings.
     *
     * @param db
     */
    public WrappingNeoServerBootstrapper( GraphDatabaseAPI db )
    {
        this( db, new ServerConfigurator( db ) );
    }

    /**
     * Create an instance with custom documentation.
     * {@link org.neo4j.server.configuration.ServerConfigurator} is written to fit well here, see its'
     * documentation.
     *
     * @param db
     * @param configurator
     */
    public WrappingNeoServerBootstrapper( GraphDatabaseAPI db, Configurator configurator )
    {
        this.db = db;
        this.configurator = configurator;
    }

    @Override
    protected Configurator createConfigurator( ConsoleLogger log )
    {
        return configurator;
    }

	@Override
	protected NeoServer createNeoServer() {
		return new WrappingNeoServer(db, configurator);
	}
}
