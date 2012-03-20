/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStoreAccess
{
    public @Rule
    TestDirectory testdir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void openingThroughStoreAccessShouldNotTriggerRecovery() throws Exception
    {
        ProduceUncleanStore.atPath( testdir.directory() );
        assertTrue( "Store should be unclean", isUnclean( testdir.directory() ) );
        File messages = new File( testdir.directory(), "messages.log" );
        long eof = messages.length();
        new StoreAccess( testdir.directory().getAbsolutePath() ).close();
        String data = readFrom( messages, eof );
        // This doesn't actually check for recovery, it checks for startup of the DB (by
        // looking in the log) and we assume that recovery would happen during DB startup.
        assertFalse( "should not have started GraphDatabase", data.contains( "STARTUP" ) );
        assertTrue( "Store should be unclean", isUnclean( testdir.directory() ) );
    }

    private boolean isUnclean( File directory ) throws IOException
    {
        char chr = activeLog( directory );
        return chr == '1' || chr == '2';
    }

    private String readFrom( File file, long start ) throws IOException
    {
        RandomAccessFile input = new RandomAccessFile( file, "r" );
        try
        {
            byte[] data = new byte[(int) ( input.length() - start )];
            input.seek( start );
            assertEquals( data.length, input.read( data ) );
            return new String( data );
        }
        finally
        {
            input.close();
        }
    }

    private char activeLog( File directory ) throws IOException
    {
        RandomAccessFile file = new RandomAccessFile( new File( directory, "nioneo_logical.log.active" ), "r" );
        try
        {
            return file.readChar();
        }
        finally
        {
            file.close();
        }
    }
}
