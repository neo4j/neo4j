/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.lineIterator;

enum MigrationStatus
{
    migrating,
    moving,
    completed;

    public boolean isNeededFor( MigrationStatus current )
    {
        return current == null || this.ordinal() >= current.ordinal();
    }

    public String maybeReadInfo( FileSystemAbstraction fs, Path stateFile, String currentInfo )
    {
        if ( currentInfo != null )
        {
            return currentInfo;
        }

        var stateInfo = readFromFile( fs, stateFile, this );
        return stateInfo.map( s -> s.state ).orElse( null );
    }

    public static MigrationStatus readMigrationStatus( FileSystemAbstraction fs, Path stateFile )
    {
        var stateInfo = readFromFile( fs, stateFile, null );
        return stateInfo.map( s -> MigrationStatus.valueOf( s.state ) ).orElse( null );
    }

    private static Optional<MigrationStateInfo> readFromFile( FileSystemAbstraction fs, Path path, MigrationStatus expectedSate )
    {
        try ( var reader = fs.openAsReader( path, UTF_8 ) )
        {
            var lineIterator = lineIterator( reader );
            String state = lineIterator.next().trim();
            if ( expectedSate != null && !expectedSate.name().equals( state ) )
            {
                throw new IllegalStateException(
                        "Not in the expected state, expected=" + expectedSate.name() + ", actual=" + state );
            }
            String info = lineIterator.next().trim();
            return Optional.of( new MigrationStateInfo( state, info ) );
        }
        catch ( NoSuchFileException e )
        {
            return Optional.empty();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void setMigrationStatus( FileSystemAbstraction fs, Path stateFile, String info )
    {
        if ( fs.fileExists( stateFile ) )
        {
            try
            {
                fs.truncate( stateFile, 0 );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        try ( Writer writer = fs.openAsWriter( stateFile, UTF_8, false ) )
        {
            writer.write( name() );
            writer.write( '\n' );
            writer.write( info );
            writer.flush();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    record MigrationStateInfo(String state, String info)
    {
    }
}
