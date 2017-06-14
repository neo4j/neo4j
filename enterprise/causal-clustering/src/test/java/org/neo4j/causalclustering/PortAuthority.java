/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering;

import static org.neo4j.causalclustering.PortConstants.EphemeralPortMinimum;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A source for free ports from the ephemeral/ dynamic port range, suitable for short lived things like tests.
 */
public class PortAuthority
{
    private static final PortProvider portProvider;

    static
    {
        String portAuthorityDirectory = System.getProperty( "port.authority.directory" );

        if ( portAuthorityDirectory == null )
        {
            portProvider = new SimplePortProvider( new DefaultPortProbe(), EphemeralPortMinimum );
        }
        else
        {
            try
            {
                Path directory = Paths.get( portAuthorityDirectory );
                Files.createDirectories( directory );
                PortRepository portRepository = new PortRepository( directory );
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
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try ( PrintWriter printWriter = new PrintWriter( outputStream ) )
        {
            new Exception().printStackTrace( printWriter );
        }

        return portProvider.getNextFreePort( new String( outputStream.toByteArray() ) );
    }
}
