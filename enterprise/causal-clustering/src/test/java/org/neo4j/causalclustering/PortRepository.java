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

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PortRepository
{
    private final Path directory;

    public PortRepository( Path directory )
    {
        this.directory = directory;
    }

    // synchronize between threads in this JVM
    public synchronized int reserveNextPort( String trace )
    {
        // if we do not find one after trying 100 times, bail out
        for ( int i = 0; i < 100; i++ )
        {
            // have OS pick a free port
            try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
            {
                int port = serverSocket.getLocalPort();

                Path portFilePath = directory.resolve( "port" + port );

                Files.createFile( portFilePath );

                try ( FileOutputStream fileOutputStream = new FileOutputStream( portFilePath.toFile(), true ) )
                {
                    fileOutputStream.write( trace.getBytes() );
                    fileOutputStream.flush();
                }

                return port;
            }
            catch ( FileAlreadyExistsException e )
            {
                // try again
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "This will never happen - LWN", e );
            }
        }

        throw new IllegalStateException( "There are no more ephemeral/ dynamic ports available" );
    }
}
