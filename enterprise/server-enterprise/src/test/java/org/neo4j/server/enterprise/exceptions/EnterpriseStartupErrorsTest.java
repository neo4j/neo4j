/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.enterprise.exceptions;

import org.junit.Test;

import org.neo4j.causalclustering.identity.BootstrapConnectionTimeout;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.AssertableLogProvider;

import static org.neo4j.kernel.lifecycle.LifecycleStatus.STARTED;
import static org.neo4j.kernel.lifecycle.LifecycleStatus.STARTING;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class EnterpriseStartupErrorsTest
{
    @Test
    public void shouldDescribeCausalClusterBootstrapTimeout()
    {
        // given
        AssertableLogProvider logging = new AssertableLogProvider();
        LifecycleException error = new LifecycleException( new Object(), STARTING, STARTED,
                new RuntimeException( "Error starting org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory",
                        new LifecycleException( new Object(), STARTING, STARTED,
                                new LifecycleException( new Object(), STARTING, STARTED,
                                        new BootstrapConnectionTimeout( "Bork bork" ) ) ) ) );

        // when
        EnterpriseStartupErrors.translateEnterpriseStartupError( error ).describeTo( logging.getLog( "console" ) );

        // then
        logging.assertExactly( inLog( "console" ).error( "Bork bork" ) );
    }
}