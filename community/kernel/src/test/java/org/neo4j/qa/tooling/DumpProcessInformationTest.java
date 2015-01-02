/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.qa.tooling;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.TargetDirectory;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.getProperty;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.Predicates.stringContains;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class DumpProcessInformationTest
{
    @Test
    public void shouldDumpProcessInformation() throws Exception
    {
        // GIVEN
        // a process spawned from this test which pauses at a specific point of execution
        Process process = getRuntime().exec( new String[] { "java", "-cp", getProperty( "java.class.path" ),
                DumpableProcess.class.getName(), SIGNAL } );
        awaitSignal( process );

        // WHEN
        // dumping process information for that spawned process (knowing it's in the expected position)
        DumpProcessInformation dumper = new DumpProcessInformation( new DevNullLoggingService(), directory );
        Pair<Long, String> pid = single( dumper.getJPids( stringContains( DumpableProcess.class.getSimpleName() ) ) );
        File threaddumpFile = dumper.doThreadDump( pid );
        process.destroy();

        // THEN
        // the produced thread dump should contain that expected method at least
        assertTrue( fileContains( threaddumpFile, "traceableMethod", DumpableProcess.class.getName() ) );
    }

    private boolean fileContains( File file, String... expectedStrings )
    {
        Set<String> expectedStringSet = asSet( expectedStrings );
        for ( String line : asIterable( file, "UTF-8" ) )
        {
            Iterator<String> expectedStringIterator = expectedStringSet.iterator();
            while ( expectedStringIterator.hasNext() )
            {
                if ( line.contains( expectedStringIterator.next() ) )
                {
                    expectedStringIterator.remove();
                }
            }
        }
        return expectedStringSet.isEmpty();
    }

    private static final String SIGNAL = "here";

    private final File directory = TargetDirectory.forTest( getClass() ).cleanDirectory( "dump" );

    private void awaitSignal( Process process ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) ) )
        {
            String line = reader.readLine();
            if ( !SIGNAL.equals( line ) )
            {
                fail( "Got weird signal " + line );
            }
            // We got signal, great
        }
    }
}
