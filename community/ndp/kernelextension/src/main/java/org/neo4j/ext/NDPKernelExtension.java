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
package org.neo4j.ext;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.ndp.runtime.internal.StandardSessions;
import org.neo4j.ndp.transport.http.HttpTransport;
import org.neo4j.ndp.transport.http.SessionRegistry;
import org.neo4j.ndp.runtime.Sessions;

import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.HOSTNAME_PORT;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.serverTransactionTimeout;

/**
 * Wraps NDP and exposes it as a Kernel Extension.
 */
@Service.Implementation(KernelExtensionFactory.class)
public class NDPKernelExtension extends KernelExtensionFactory<NDPKernelExtension.Dependencies>
{
    public static class Settings
    {
        @Description( "Max time that sessions can be idle, after this interval a session will get closed." )
        public static final Setting<Boolean> ndp_enabled = setting("experimental.ndp.enabled", BOOLEAN,
                "false" );

        @Description( "Max time that sessions can be idle, after this interval a session will get closed." )
        public static final Setting<Long> session_max_idle = setting("dbms.session.max_idle_time", DURATION, "300s" );

        @Description( "Host and port for the Neo4j Data Protocol http transport" )
        public static final Setting<HostnamePort> http_address =
                setting("dbms.ndp.address", HOSTNAME_PORT, "localhost:7687" );
    }

    public interface Dependencies
    {
        JobScheduler scheduler();
        Logging logging();
        Config config();
        GraphDatabaseService db();
    }

    public NDPKernelExtension()
    {
        super( "neo4j-data-protocol-server" );
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        final Config config = dependencies.config();
        final GraphDatabaseService gdb = dependencies.db();
        final GraphDatabaseAPI api = (GraphDatabaseAPI) gdb;
        final StringLogger log = dependencies.logging().getMessagesLog( Sessions.class );
        final JobScheduler scheduler = dependencies.scheduler();
        final LifeSupport life = new LifeSupport();

        if(config.get( Settings.ndp_enabled ))
        {
            final Sessions env = life.add( new StandardSessions( api, log ) );
            final SessionRegistry sessions = life.add( new SessionRegistry( env ) );

            // Start services

            scheduler.scheduleRecurring( serverTransactionTimeout, new Runnable()
            {
                @Override
                public void run()
                {
                    sessions.destroyIdleSessions( config.get( Settings.session_max_idle ), TimeUnit.MILLISECONDS );
                }
            }, 5, TimeUnit.SECONDS );

            // create listeners
            HostnamePort bindTo = config.get( Settings.http_address );
            life.add( new HttpTransport( bindTo.getHost( "localhost" ), bindTo.getPort(), sessions, log ) );
            log.info( "NDP Server extension loaded." );
        }

        return life;
    }
}
