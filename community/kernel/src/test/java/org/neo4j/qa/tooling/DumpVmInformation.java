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
package org.neo4j.qa.tooling;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.Thread.State;

public class DumpVmInformation
{
    public static void dumpVmInfo( File directory ) throws IOException
    {
        File file = new File( directory, "main-vm-dump-" + System.currentTimeMillis() );
        PrintStream out = null;
        try
        {
            out = new PrintStream( file );
            dumpVmInfo( out );
        }
        finally
        {
            if ( out != null )
                out.close();
        }
    }
    
    public static void dumpVmInfo( PrintStream out )
    {
        // Find the top thread group
        ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();
        while ( topThreadGroup.getParent() != null )
            topThreadGroup = topThreadGroup.getParent();

        // Get all the thread groups under the top.
        ThreadGroup[] allGroups = new ThreadGroup[1000];
        topThreadGroup.enumerate( allGroups, true );
        
        // Dump the info.
        for ( ThreadGroup group : allGroups )
        {
            if ( group == null )
                break;
            dumpThreadGroupInfo( group, out );
        }
        dumpThreadGroupInfo( topThreadGroup, out );
    }

    public static void dumpThreadGroupInfo( ThreadGroup tg, PrintStream out )
    {
        String parentName = (tg.getParent() == null ? null : tg.getParent().getName());
        // Dump thread group info.
        out.println( "---- GROUP:" + tg.getName() +
                (parentName != null ? " parent:" + parentName : "" ) +
                (tg.isDaemon() ? " daemon" : "" ) +
                (tg.isDestroyed() ? " destroyed" : "" ) +
                " ----" );
        // Dump info for each thread.
        Thread[] allThreads = new Thread[1000];
        tg.enumerate( allThreads, false );
        for ( Thread thread : allThreads )
        {
            if ( thread == null )
                break;
            out.println(
                    "\"" + thread.getName() + "\"" +
                    (thread.isDaemon() ? " daemon" : "") +
            		" prio=" + thread.getPriority() +
            		" tid=" + thread.getId() +
            		" " + thread.getState().name().toLowerCase() );
            out.println( "  " + State.class.getName() + ": " + thread.getState().name() );
            for ( StackTraceElement element : thread.getStackTrace() )
            {
                out.println( "\tat " + element );
            }
        }
    } 
}
