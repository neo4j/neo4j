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
package org.neo4j.server.arbiter;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.server.ServerCommandLineArgs;
import org.neo4j.server.enterprise.ArbiterBootstrapper;

public class ArbiterBootstrapperTestProxy
{
    public static final String START_SIGNAL = "starting";

    private ArbiterBootstrapperTestProxy()
    {
    }

    public static void main( String[] argv ) throws IOException
    {
        ServerCommandLineArgs args = ServerCommandLineArgs.parse( argv );

        // This sysout will be intercepted by the parent process and will trigger
        // a start of a timeout. The whole reason for this class to be here is to
        // split awaiting for the process to start and actually awaiting the cluster client to start.
        System.out.println( START_SIGNAL );
        try ( ArbiterBootstrapper arbiter = new ArbiterBootstrapper() )
        {
            arbiter.start( args.homeDir(), args.configFile(), Collections.emptyMap() );
            System.in.read();
        }
    }
}
