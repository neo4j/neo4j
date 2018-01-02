/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.ports.allocation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A source for free ports on this machine
 */
public class PortAuthority
{
    // this is quite an arbitrary choice and not currently configurable - but it works.
    private static final int PORT_RANGE_MINIMUM = 20000;

    private static final PortProvider portProvider;

    static
    {
        String portAuthorityDirectory = System.getProperty( "port.authority.directory" );

        if ( portAuthorityDirectory == null )
        {
            portProvider = new SimplePortProvider( new DefaultPortProbe(), PORT_RANGE_MINIMUM );
        }
        else
        {
            try
            {
                Path directory = Paths.get( portAuthorityDirectory );
                Files.createDirectories( directory );
                PortRepository portRepository = new PortRepository( directory, PORT_RANGE_MINIMUM );
                PortProbe portProbe = new DefaultPortProbe();
                portProvider = new CoordinatingPortProvider( portRepository, portProbe );
            }
            catch ( IOException e )
            {
                throw new ExceptionInInitializerError( e );
            }
        }
    }

    public static int allocatePort()
    {
        String trace = buildTrace();

        return portProvider.getNextFreePort( trace );
    }

    private static String buildTrace()
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try ( PrintWriter printWriter = new PrintWriter( outputStream ) )
        {
            new Exception().printStackTrace( printWriter );
        }

        return new String( outputStream.toByteArray() );
    }
}
