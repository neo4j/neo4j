/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.enterprise.helpers;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.EphemeralDatabase;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.paging.Clock;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.paging.RealClock;
import org.neo4j.server.rest.web.DatabaseActions;

import static org.neo4j.server.ServerTestUtils.createTempDir;

public class EnterpriseServerBuilder extends ServerBuilder
{
    public static EnterpriseServerBuilder server()
    {
        return new EnterpriseServerBuilder();
    }

    @Override
    public EnterpriseNeoServer build() throws IOException
    {
        if ( dbDir == null )
        {
            this.dbDir = createTempDir().getAbsolutePath();
        }
        final File configFile = createPropertiesFiles();

        if ( preflightTasks == null )
        {
            preflightTasks = new PreFlightTasks()
            {
                @Override
                public boolean run()
                {
                    return true;
                }
            };
        }

        return new EnterpriseNeoServer( new PropertyFileConfigurator( new Validator(
                new DatabaseLocationMustBeSpecifiedRule() ), configFile ) )
        {
            @Override
            protected PreFlightTasks createPreflightTasks()
            {
                return preflightTasks;
            }

            @Override
            protected Database createDatabase()
            {
                return persistent ?
                        super.createDatabase() :
                        new EphemeralDatabase( configurator.configuration() );
            }

            @Override
            protected DatabaseActions createDatabaseActions()
            {
                Clock clockToUse = (clock != null) ? clock : new RealClock();

                return new DatabaseActions( database,
                        new LeaseManager( clockToUse ),
                        ForceMode.forced,
                        configurator.configuration().getBoolean(
                                Configurator.SCRIPT_SANDBOXING_ENABLED_KEY,
                                Configurator.DEFAULT_SCRIPT_SANDBOXING_ENABLED ) );
            }
            
            @Override
            public void stop()
            {
                configFile.delete();
            }
        };
    }
}
