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
    private final PrintWriter out;
    
    private StringLogger( String filename )
    {
        try
        {
            File file = new File( filename );
            file.getParentFile().mkdirs();
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
    
    public static StringLogger getLogger( String filename )
    {
        StringLogger logger = loggers.get( filename );
        if ( logger == null )
        {
            if ( filename == null || filename.startsWith( "null" ) )
            {
                logger = new StringLogger( new PrintWriter( System.out ) );
            }
            else
            {
                logger = new StringLogger( filename );
            }
            loggers.put( filename, logger );
        }
        return logger;
    }
    
    public synchronized void logMessage( String msg )
    {
        out.println( new Date() + ": " + msg );
        out.flush();
    } 

    public synchronized void logMessage( String msg, Throwable cause )
    {
        out.println( new Date() + ": " + msg + " " + cause.getMessage() );
        cause.printStackTrace( out );
        out.flush();
    }
    
    public synchronized static void close( String filename )
    {
        StringLogger logger = loggers.remove( filename );
        if ( logger != null )
        {
            logger.out.close();
        }
    }
}
