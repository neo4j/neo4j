/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.advanced;

import java.util.Arrays;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.advanced.modules.JMXManagementModule;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.database.Database;
import org.neo4j.server.modules.ServerModule;

public class AdvancedNeoServer extends CommunityNeoServer
{
    public AdvancedNeoServer( ConfigurationBuilder configurator, Database.Factory dbFactory, InternalAbstractGraphDatabase.Dependencies dependencies)
    {
        super(configurator, dbFactory, dependencies);
    }

	public AdvancedNeoServer( ConfigurationBuilder configurator, InternalAbstractGraphDatabase.Dependencies dependencies)
    {
        super( configurator, dependencies );
    }

    @Override
	protected Iterable<ServerModule> createServerModules()
	{
        return Iterables.mix(Arrays.asList(
        		(ServerModule)new JMXManagementModule(this)),
        		super.createServerModules());
	}
}
