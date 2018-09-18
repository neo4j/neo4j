/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.test.rule.DatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.io.ByteUnit.mebiBytes;

public class SabotageNativeIndex implements DatabaseRule.RestartAction
{
    private final Random random;

    public SabotageNativeIndex( Random random )
    {
        this.random = random;
    }

    @Override
    public void run( FileSystemAbstraction fs, DatabaseLayout databaseLayout ) throws IOException
    {
        int files = scrambleIndexFiles( fs, nativeIndexDirectoryStructure( databaseLayout ).rootDirectory(), 0 );
        assertThat( files, greaterThanOrEqualTo( 1 ) );
    }

    private int scrambleIndexFiles( FileSystemAbstraction fs, File fileOrDir, int count ) throws IOException
    {
        if ( fs.isDirectory( fileOrDir ) )
        {
            File[] children = fs.listFiles( fileOrDir );
            if ( children != null )
            {
                for ( File child : children )
                {
                    return scrambleIndexFiles( fs, child, count );
                }
            }
        }
        else
        {
            // Completely scramble file, assuming small files
            System.out.println( "scrambling " + fileOrDir );
            try ( StoreChannel channel = fs.open( fileOrDir, OpenMode.READ_WRITE ) )
            {
                if ( channel.size() > mebiBytes( 10 ) )
                {
                    throw new IllegalArgumentException( "Was expecting small files here" );
                }
                ByteBuffer buffer = ByteBuffer.allocate( (int) channel.size() );
                channel.readAll( buffer );
                buffer.flip();
                for ( int i = 0; i < buffer.limit(); i++ )
                {
                    byte existing = buffer.get( i );
                    byte scrambled = existing;
                    while ( scrambled == existing )
                    {
                        scrambled = (byte) random.nextInt();
                    }
                    buffer.put( i, scrambled );
                }
                channel.position( 0 );
                channel.writeAll( buffer );
            }
            count++;
        }
        return count;
    }

    public static IndexDirectoryStructure nativeIndexDirectoryStructure( DatabaseLayout databaseLayout )
    {
        return IndexDirectoryStructure.directoriesByProvider( databaseLayout.databaseDirectory() ).forProvider(
                GenericNativeIndexProvider.DESCRIPTOR );
    }
}
