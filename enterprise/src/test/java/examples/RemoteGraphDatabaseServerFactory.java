/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package examples;

// START SNIPPET: class
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.transports.LocalGraphDatabase;

public class RemoteGraphDatabaseServerFactory
{
    private GraphDatabaseService graphDb;
    private Map<String, IndexService> indexes = new HashMap<String, IndexService>();

    public RemoteGraphDatabaseServerFactory( GraphDatabaseService graphDb )
    {
        this.graphDb = graphDb;
    }

    public BasicGraphDatabaseServer create()
    {
        BasicGraphDatabaseServer server = new LocalGraphDatabase( graphDb );
        for ( Map.Entry<String, IndexService> entry : indexes.entrySet() )
        {
            server.registerIndexService( entry.getKey(), entry.getValue() );
        }
        return server;
    }

    public RemoteGraphDatabaseServerFactory addIndex( String id,
            IndexService service )
    {
        indexes.put( id, service );
        return this;
    }
}
// END SNIPPET: class
