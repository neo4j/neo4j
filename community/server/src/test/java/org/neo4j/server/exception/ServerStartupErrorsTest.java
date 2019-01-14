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
package org.neo4j.server.exception;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.AssertableLogProvider;

import static org.neo4j.kernel.lifecycle.LifecycleStatus.STARTED;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.STARTING;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.exception.ServerStartupErrors.translateToServerStartupError;

public class ServerStartupErrorsTest
{
    @Test
    public void shouldDescribeUpgradeFailureInAFriendlyWay()
    {
        // given
        AssertableLogProvider logging = new AssertableLogProvider();
        LifecycleException error = new LifecycleException( new Object(), STARTING, STARTED,
                new RuntimeException( "Error starting org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory",
                        new LifecycleException( new Object(), STARTING, STARTED,
                                new LifecycleException( new Object(), STARTING, STARTED,
                                        new UpgradeNotAllowedByConfigurationException() ) ) ) );

        // when
        translateToServerStartupError( error ).describeTo( logging.getLog( "console" ) );

        // then
        logging.assertExactly( inLog( "console" )
                .error( "Neo4j cannot be started because the database files require upgrading and upgrades are disabled " +
                        "in the configuration. Please set '" + GraphDatabaseSettings.allow_upgrade.name() + "' to 'true' " +
                        "in your configuration file and try again." ) );
    }
}
