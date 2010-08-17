package org.neo4j.shell.apps;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;

@Service.Implementation( App.class )
public class Alias extends AbstractApp
{
    public static final String ALIAS_PREFIX = "ALIAS_";

    @Override
    public String getDescription()
    {
        return "Adds an alias so that it can be used later as a command.\n" +
                "Usage: alias <key>='<value>'";
    }

    public String execute( AppCommandParser parser, Session session,
            Output out ) throws Exception
    {
        String line = parser.getLineWithoutApp();
        if ( line.trim().length() == 0 )
        {
            printAllAliases( session, out );
            return null;
        }

        String[] keyValue = Export.splitInKeyEqualsValue( line );
        String key = ALIAS_PREFIX + keyValue[ 0 ];
        String value = keyValue[ 1 ];
        if ( value == null || value.trim().length() == 0 )
        {
            safeRemove( session, key );
        }
        else
        {
            safeSet( session, key, value );
        }
        return null;
    }

    private void printAllAliases( Session session, Output out )
            throws Exception
    {
        for ( String key : session.keys() )
        {
            if ( !key.startsWith( ALIAS_PREFIX ) )
            {
                continue;
            }
            String shortKey = key.substring( ALIAS_PREFIX.length() );
            out.println( "alias " + shortKey + "='" + session.get( key ) +
                    "'" );
        }
    }
}
