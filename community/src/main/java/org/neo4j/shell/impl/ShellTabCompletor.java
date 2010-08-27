package org.neo4j.shell.impl;

import java.rmi.RemoteException;
import java.util.List;

import jline.Completor;
import jline.SimpleCompletor;

import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.TabCompletion;

class ShellTabCompletor implements Completor
{
    private final ShellClient client;
    
    private long timeWhenCached;
    private Completor appNameCompletor;

    public ShellTabCompletor( ShellClient client )
    {
        this.client = client;
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
                TabCompletion completion = client.getServer().tabComplete( buffer.trim(),
                        client.session() );
                cursor = completion.getCursor();
                candidates.addAll( completion.getCandidates() );
            }
            else
            {
                // Complete the app name
                return getAppNameCompletor().complete( buffer, cursor, candidates );
            }
        }
        catch ( RemoteException e )
        {
            // TODO Throw something?
            e.printStackTrace();
        }
        catch ( ShellException e )
        {
            // TODO Throw something?
            e.printStackTrace();
        }
        return cursor;
    }
    
    private Completor getAppNameCompletor() throws RemoteException
    {
        if ( timeWhenCached != client.timeForMostRecentConnection() )
        {
            timeWhenCached = client.timeForMostRecentConnection();
            appNameCompletor = new SimpleCompletor( client.getServer().getAllAvailableCommands() );
        }
        return this.appNameCompletor;
    }
}
