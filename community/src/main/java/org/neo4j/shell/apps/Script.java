package org.neo4j.shell.apps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.AbstractApp;

public class Script extends AbstractApp
{
    {
        addOptionDefinition( "v", new OptionDefinition( OptionValueType.NONE,
                "Verbose, print commands" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Executes a script of shell commands, supply a file name " +
        		"containing the script";
    }
    
    public String execute( AppCommandParser parser, Session session,
            Output out ) throws ShellException
    {
        boolean verbose = parser.options().containsKey( "v" );
        File file = new File( parser.arguments().get( 0 ) );
        BufferedReader reader = null;
        try
        {        
            if ( !file.exists() )
            {
                out.println( "Couldn't find file '" +
                        file.getAbsolutePath() + "'" );
                return null;
            }
            reader = new BufferedReader( new FileReader( file ) );
            String line = null;
            int counter = 0;
            while ( ( line = reader.readLine() ) != null )
            {
                if ( verbose )
                {
                    if ( counter++ > 0 )
                    {
                        out.println();
                    }
                    out.println( "[" + line + "]" );
//                    out.println();
                }
                getServer().interpretLine( line, session, out );
            }
        }
        catch ( IOException e )
        {
            throw new ShellException( e );
        }
        finally
        {
            safeClose( reader );
        }
        return null;
    }
}
