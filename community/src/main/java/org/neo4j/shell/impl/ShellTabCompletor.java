package org.neo4j.shell.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import jline.Completor;
import jline.SimpleCompletor;

import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.AppShellServer;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.TextUtil;

class ShellTabCompletor implements Completor
{
    private final ShellClient client;
    private final Completor appNameCompletor;

    public ShellTabCompletor( ShellClient client ) throws RemoteException
    {
        this.client = client;
        this.appNameCompletor = new SimpleCompletor( client.getServer().getAllAvailableCommands() );
    }
    
    public int complete( String buffer, int cursor, List candidates )
    {
        if ( buffer == null || buffer.length() == 0 )
        {
            return cursor;
        }
        
        try
        {
            if ( buffer.contains( " " ) )
            {
                // Complete the argument to app
                // TODO We can't assume it's an AppShellServer, can we?
                AppCommandParser parser = new AppCommandParser(
                        (AppShellServer) client.getServer(), buffer.toString() );
                App app = parser.app();
                List<String> appCandidates = app.completionCandidates( buffer, client.session() );
                appCandidates = quote( appCandidates );
                if ( appCandidates.size() == 1 )
                {
                    appCandidates.set( 0, appCandidates.get( 0 ) + " " );
                }
                candidates.addAll( appCandidates );
                return buffer.length() - TextUtil.lastWordOrQuoteOf( buffer, true ).length();
            }
            else
            {
                // Complete the app name
                return this.appNameCompletor.complete( buffer, cursor, candidates );
            }
        }
        catch ( ShellException e )
        {
            // TODO Throw something?
            e.printStackTrace();
        }
        return cursor;
    }

    private List<String> quote( List<String> candidates )
    {
        List<String> result = new ArrayList<String>();
        for ( String candidate : candidates )
        {
            candidate = candidate.replaceAll( " ", "\\\\ " );
            result.add( candidate );
        }
        return result;
    }
}
