/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Predicates.stringContains;

/**
 * Used to dump information (such as thread dump, heap dump) of a java process running on the local machine.
 */
public class DumpProcessInformation
{
    public static void main( String[] args ) throws Exception
    {
        Args arg = new Args( args == null ? new String[0] : args );
        boolean doHeapDump = arg.getBoolean( "heap", false, true );
        Predicate<String> processFilter = arg.orphans().isEmpty() ?
                Predicates.<String>TRUE() : stringContains( arg.orphans().get( 0 ) );
        String dumpDir = arg.get( "dir", "data" );
        File dirFile = dumpDir != null ? new File( dumpDir ) : null;
        dirFile.mkdirs();
        for ( Pair<Long, String> pid : getJPids( processFilter ) )
        {
            doThreadDump( pid, dirFile );
            if ( doHeapDump )
                doHeapDump( pid, dirFile );
        }
    }

    public static void doThreadDump( Predicate<String> processFilter, File outputDirectory ) throws Exception
    {
        for ( Pair<Long,String> pid : getJPids( processFilter ) )
        {
            doThreadDump( pid, outputDirectory );
        }
    }
    
    public static File doThreadDump( Pair<Long, String> pid, File outputDirectory ) throws Exception
    {
        String[] cmdarray = new String[] {"jstack", "" + pid.first()};
        File outputFile = new File( outputDirectory, "threaddump-" + pid.other() + "-" + currentTimeMillis() );
        Process process = getRuntime().exec( cmdarray );
        writeProcessOutputToFile( process, outputFile );
        return outputFile;
    }

    private static void writeProcessOutputToFile( Process process, File outputFile ) throws Exception
    {
        BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
        String line = null;
        PrintStream out = new PrintStream( outputFile );
        while ( (line = reader.readLine()) != null )
        {
            out.println( line );
        }
        out.close();
        process.waitFor();
    }

    private static void doHeapDump( Pair<Long, String> pid, File dir ) throws Exception
    {
        String[] cmdarray = new String[] {"jmap", "-dump:file=" + new File( dir, "heapdump-" + pid.other() +
                "-" + currentTimeMillis() ).getAbsolutePath(), "" + pid.first() };
        getRuntime().exec( cmdarray ).waitFor();
    }
    
    public static Collection<Pair<Long, String>> getJPids( Predicate<String> processFilter )
            throws IOException, InterruptedException
    {
        Process process = getRuntime().exec( new String[] { "jps", "-l" } );
        BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
        Collection<Pair<Long, String>> jPids = new ArrayList<Pair<Long,String>>();
        for ( String line = null; (line = reader.readLine()) != null; )
        {
            int spaceIndex = line.indexOf( ' ' );
            String name = line.substring( spaceIndex + 1 );
            // Work-around for a windows problem where if your java.exe is in a directory
            // containing spaces the value in the second column from jps output will be
            // something like "C:\Program" if it was under "C:\Program Files\Java..."
            // If that's the case then use the PID instead
            if ( name.contains( ":" ) )
            {
                String pid = line.substring( 0, spaceIndex );
                name = pid;
            }
            
            if ( name.contains( DumpProcessInformation.class.getSimpleName() ) || name.contains( "Jps" ) ||
                    name.contains( "eclipse.equinox" ) )
            {
                continue;
            }
            if ( processFilter.accept( name ) )
            {
                jPids.add( Pair.of( Long.parseLong( line.substring( 0, spaceIndex ) ), name ) );
            }
        }
        process.waitFor();
        return jPids;
    }
}
