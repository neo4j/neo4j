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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class StringLogger
{
    public static final StringLogger SYSTEM = 
        new StringLogger( new PrintWriter( System.out ) );
    
    private final PrintWriter out;
    
    private StringLogger( String filename )
    {
        try
        {
            File file = new File( filename );
            if ( file.getParentFile() != null )
            {
                file.getParentFile().mkdirs();
            }
            out = new PrintWriter( new FileWriter( file, true ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private StringLogger( PrintWriter writer )
    {
        this.out = writer;
    }
    
    private static final Map<String,StringLogger> loggers = 
        new HashMap<String, StringLogger>();
    
    public static StringLogger getLogger( String storeDir )
    {
        if ( storeDir == null )
        {
            return SYSTEM;
        }
        
        String filename = defaultFileName( storeDir );
        StringLogger logger = loggers.get( filename );
        if ( logger == null )
        {
            logger = new StringLogger( filename );
            loggers.put( filename, logger );
        }
        return logger;
    }
    
    private static String defaultFileName( String storeDir )
    {
        return new File( storeDir, "messages.log" ).getAbsolutePath();
    }
    
    public void logMessage( String msg )
    {
        logMessage( msg, false );
    } 

    public void logMessage( String msg, Throwable cause )
    {
        logMessage( msg, cause, false );
    }
    
    public void logMessage( String msg, boolean flush )
    {
        out.println( new Date() + ": " + msg );
        if ( flush )
        {
            out.flush();
        }
    } 

    public void logMessage( String msg, Throwable cause, boolean flush )
    {
        out.println( new Date() + ": " + msg + " " + cause.getMessage() );
        cause.printStackTrace( out );
        if ( flush )
        {
            out.flush();
        }
    }
    
    public void flush()
    {
        out.flush();
    }
    
    public synchronized static void close( String storeDir )
    {
        StringLogger logger = loggers.remove( defaultFileName( storeDir ) );
        if ( logger != null )
        {
            logger.out.close();
        }
    }
}
