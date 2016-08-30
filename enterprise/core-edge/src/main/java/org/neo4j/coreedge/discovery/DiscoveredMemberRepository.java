/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class DiscoveredMemberRepository
{
    private final String filename = "DiscoveredMemberAddresses.txt";
    private final FileSystemAbstraction fileSystem;
    private final File file;
    private final Log log;

    public DiscoveredMemberRepository( File directory, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this.file = new File( directory, filename );
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized Set<AdvertisedSocketAddress> previouslyDiscoveredMembers()
    {
        if ( fileSystem.fileExists( file ) )
        {
            try ( BufferedReader reader = new BufferedReader( fileSystem.openAsReader( file, UTF_8 ) ) )
            {
                return reader.lines().map( AdvertisedSocketAddress::new ).collect( toSet() );
            }
            catch ( IOException e )
            {
                log.warn( String.format( "Failed to read previously discovered members from %s ",
                        file.getAbsolutePath() ), e );
            }
        }
        return emptySet();
    }

    public synchronized void store( Set<AdvertisedSocketAddress> discoveredMembers )
    {
        try ( PrintWriter writer = new PrintWriter( fileSystem.openAsWriter( file, UTF_8, false ) ) )
        {
            for ( AdvertisedSocketAddress member : discoveredMembers )
            {
                writer.println( member.toString() );
            }
        }
        catch ( IOException e )
        {
            log.warn( String.format( "Failed to store discovered members to %s ",
                    file.getAbsolutePath() ), e );
        }
    }
}
