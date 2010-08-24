package org.neo4j.kernel;

import java.util.Collection;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

public class Hainfo extends GraphDatabaseApp
{
    @Override
    protected String exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) getServer().getDb();
        MasterServer master = db.getMasterServerIfMaster();
        out.println( "I'm currently " + (master != null ? "master" : "slave") );
        
        if ( master != null )
        {
            out.println( "Connected slaves:" );
            for ( Pair<Integer, Collection<Integer>> entry : master.getConnectedClients() )
            {
                out.println( "\tMachine ID: " + entry.first() );
                if ( entry.other() != null && !entry.other().isEmpty() )
                {
                    out.println( "\tOngoing transactions: " + entry.other() );
                }
            }
        }
        return null;
    }
}
