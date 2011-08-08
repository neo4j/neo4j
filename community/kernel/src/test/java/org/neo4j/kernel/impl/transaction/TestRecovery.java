/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;

import org.junit.Test;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.subprocess.SubProcess;

public class TestRecovery
{
    @Test
    public void messagesLogShouldContainAppliedTransactions() throws Exception
    {
        String path = "target/test-data/recovery";
        deleteRecursively( new File( path ) );
        DatabaseAsSubProcess process = new ServerProcess().start( path );
        process.awaitStarted();
        int numberOfTransactions = 10;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            process.createNode();
        }
        SubProcess.kill( process );
        Collection<String> toLookFor = asList( "Injected one phase commit" );
        assertEquals( 0, countMentionsInMessagesLog( path, toLookFor ) );
        
        new EmbeddedGraphDatabase( path ).shutdown();
        assertEquals( numberOfTransactions, countMentionsInMessagesLog( path, toLookFor ) );
    }

    public static int countMentionsInMessagesLog( String path, Collection<String> linesContaining ) throws Exception
    {
        File messageLogFile = new File( path, "messages.log" );
        BufferedReader reader = new BufferedReader( new FileReader( messageLogFile ) );
        String line = null;
        int counter = 0;
        while ( ( line = reader.readLine() ) != null )
        {
            // These strings are copied from XaLogicalLog
            for ( String contains : linesContaining )
            {
                if ( line.contains( contains ) )
                {
                    counter++;
                    break;
                }
            }
        }
        reader.close();
        return counter;
    }
}
