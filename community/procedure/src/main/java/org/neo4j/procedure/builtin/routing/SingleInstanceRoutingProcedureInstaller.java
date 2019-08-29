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
package org.neo4j.procedure.builtin.routing;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.logging.LogProvider;

public class SingleInstanceRoutingProcedureInstaller extends BaseRoutingProcedureInstaller
{
    protected final DatabaseManager<?> databaseManager;
    protected final ConnectorPortRegister portRegister;
    protected final Config config;
    protected final LogProvider logProvider;

    public SingleInstanceRoutingProcedureInstaller( DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister,
            Config config, LogProvider logProvider )
    {
        this.databaseManager = databaseManager;
        this.portRegister = portRegister;
        this.config = config;
        this.logProvider = logProvider;
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        return new SingleInstanceGetRoutingTableProcedure( namespace, databaseManager, portRegister, config, logProvider );
    }
}
