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
package org.neo4j.shell.apps.ha;

import java.util.Collection;
import java.util.Map;

import org.neo4j.com.SlaveContext;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.kernel.apps.ReadOnlyGraphDatabaseApp;

public class Hainfo extends ReadOnlyGraphDatabaseApp
{
    @Override
    protected String exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) getServer().getDb();
        MasterServer master = db.getMasterServerIfMaster();
        out.println( "I'm currently " + (db.isMaster() ? "master" : "slave") );
        
        if ( master != null )
        {
            out.println( "Connected slaves:" );
            for ( Map.Entry<Integer, Collection<SlaveContext>> entry :
                    master.getSlaveInformation().entrySet() )
            {
                out.println( "\tMachine ID " + entry.getKey() );
                if ( entry.getValue() != null )
                {
                    for ( SlaveContext tx : entry.getValue() )
                    {
                        out.println( "\t\tRunning tx: " + tx );
                    }
                }
            }
        }
        return null;
    }
}
