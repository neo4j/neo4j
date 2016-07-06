/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.string.UTF8;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public abstract class FileRepositorySerializer<S>
{
    public void saveRecordsToFile(Path recordsFile, Collection<S> records) throws IOException
    {
        Path directory = recordsFile.getParent();
        if ( !Files.exists( directory ) )
        {
            Files.createDirectories( directory );
        }

        Path tempFile = Files.createTempFile( directory, recordsFile.getFileName().toString() + "-", ".tmp" );
        try
        {
            Files.write( tempFile, serialize( records ) );
            Files.move( tempFile, recordsFile, ATOMIC_MOVE, REPLACE_EXISTING );
        }
        catch ( Throwable e )
        {
            Files.delete( tempFile );
            throw e;
        }
    }

    public List<S> loadRecordsFromFile(Path recordsFile) throws IOException, FormatException
    {
        byte[] fileBytes = Files.readAllBytes( recordsFile );
        return deserializeRecords( fileBytes );
    }

    public byte[] serialize(Collection<S> records)
    {
        StringBuilder sb = new StringBuilder();
        for ( S record : records )
        {
            sb.append( serialize( record ) ).append( "\n" );
        }
        return UTF8.encode( sb.toString() );
    }

    public List<S> deserializeRecords( byte[] bytes ) throws FormatException
    {
        List<S> out = new ArrayList<>(  );
        int lineNumber = 1;
        for ( String line : UTF8.decode( bytes ).split( "\n" ) )
        {
            if ( line.trim().length() > 0 )
            {
                out.add( deserializeRecord( line, lineNumber ) );
            }
            lineNumber++;
        }
        return out;
    }

    protected abstract String serialize( S record );

    protected abstract S deserializeRecord( String line, int lineNumber ) throws FormatException;
}
