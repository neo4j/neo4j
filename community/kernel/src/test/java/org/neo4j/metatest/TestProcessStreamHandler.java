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
package org.neo4j.metatest;

import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.neo4j.test.ProcessStreamHandler;

public class TestProcessStreamHandler
{
    @Test
    public void testKillProcess() throws Exception
    {
        Process process = Runtime.getRuntime().exec( new String[] { "java", "-cp", getProperty( "java.class.path" ),
                ProcessThatJustWaits.class.getName(), "" + 20 } );
        ProcessStreamHandler handler = new ProcessStreamHandler( process, true );
        try
        {
            handler.waitForResult( 1 );
            fail( "Should fail awaiting the process" );
        }
        catch ( Exception e )
        {   // Good
        }
        assertEquals( "Process should have been destroyed by now with exit code 1", 1, process.exitValue() );
    }
}
