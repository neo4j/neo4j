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
package org.neo4j.kernel.impl.storemigration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;

import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;

enum MigrationStatus
{
    migrating,
    moving,
    countsRebuilding,
    completed;

    public boolean isNeededFor( MigrationStatus current )
    {
        return current == null || this.ordinal() >= current.ordinal();
    }

    public String maybeReadInfo( FileSystemAbstraction fs, File stateFile, String currentInfo )
    {
        if ( currentInfo != null )
        {
            return currentInfo;
        }

        Pair<String,String> data = readFromFile( fs, stateFile, this );
        return data == null ? null : data.other();
    }

    public static MigrationStatus readMigrationStatus( FileSystemAbstraction fs, File stateFile )
    {
        Pair<String,String> data = readFromFile( fs, stateFile, null );
        if ( data == null )
        {
            return null;
        }

        return MigrationStatus.valueOf( data.first() );
    }

    private static Pair<String, String> readFromFile( FileSystemAbstraction fs, File file, MigrationStatus expectedSate )
    {
        try ( BufferedReader reader = new BufferedReader( fs.openAsReader( file, "utf-8" ) ) )
        {
            String state = reader.readLine().trim();
            if ( expectedSate != null && !expectedSate.name().equals( state ) )
            {
                throw new IllegalStateException(
                        "Not in the expected state, expected=" + expectedSate.name() + ", actual=" + state );
            }
            String info = reader.readLine().trim();
            return Pair.of( state, info );
        }
        catch ( FileNotFoundException e )
        {
            return null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void setMigrationStatus( FileSystemAbstraction fs, File stateFile, String info )
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

        try ( Writer writer = fs.openAsWriter( stateFile, "utf-8", false ) )
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
}
