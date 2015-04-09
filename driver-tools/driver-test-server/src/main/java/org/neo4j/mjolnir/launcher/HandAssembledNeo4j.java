/**
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
package org.neo4j.mjolnir.launcher;

import java.io.File;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.ndp.runtime.Sessions;
import org.neo4j.ndp.runtime.internal.StandardSessions;
import org.neo4j.ndp.transport.socket.SocketTransport;

import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.HOSTNAME_PORT;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.setting;

/**
 * Assembles a Neo4j DBMS and provides the life support for starting and stopping it.
 */
public class HandAssembledNeo4j implements Lifecycle
{
    private final LifeSupport life = new LifeSupport();
    private final Config config;

    private GraphDatabaseAPI gdb = null;
    private GraphDatabaseBuilder dbBuilder;
    private LogService logService;
    private Log log;

    public static class Settings
    {
        @Description( "Path to a folder where Neo4j will store data files." )
        public static final Setting<File> data_dir = setting( "dbms.datadir", PATH, "./neo4j" );

        @Description( "Max time that sessions can be idle, after this interval a session will get closed." )
        public static final Setting<Long> session_max_idle = setting("dbms.session.max_idle_time", DURATION, "60s" );

        @Description( "Host and port for the http listener" )
        public static final Setting<HostnamePort> ndp_address = setting( "dbms.mjolnir.address", HOSTNAME_PORT,
                "localhost:7687" );
    }

    public HandAssembledNeo4j( Config config )
    {
        this.config = config;
    }

    @Override
    public void init() throws Throwable
    {

        File datadir = config.get( Settings.data_dir );
        datadir.mkdirs();
        dbBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( new File( datadir, "default.graphdb" )
                .getAbsolutePath() );
    }

    @Override
    public void start() throws Throwable {

        // This block below should be in init, but because neo starts in its constructor, we can't have neo there
        // until that's been changed. We can modify this later by having a thin wrapper that exposes gdb as a set
        // of lifecycled databases.
        gdb = (GraphDatabaseAPI) dbBuilder.setConfig( config.getParams() ).newGraphDatabase();

        logService = gdb.getDependencyResolver().resolveDependency( LogService.class );
        log = logService.getInternalLog( Sessions.class );

        final StandardSessions env = life.add( new StandardSessions( gdb, log ) );

        HostnamePort bindTo = config.get( Settings.ndp_address );
        life.add( new SocketTransport( bindTo, log, env ) );

        // Start things up
        life.start();

        System.out.println( "Neo4j started" );
    }

    @Override
    public void stop()
    {
        try
        {
            life.stop();
        }
        finally
        {
            gdb.shutdown();
        }
        System.out.println( "Neo4j stopped" );
    }

    @Override
    public void shutdown() throws Throwable
    {

    }
}
