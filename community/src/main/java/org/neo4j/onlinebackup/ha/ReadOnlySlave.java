/*
 * Copyright (c) 2009-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.onlinebackup.ha;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.onlinebackup.net.Callback;

public class ReadOnlySlave extends AbstractSlave implements Callback
{
    public ReadOnlySlave( String path, Map<String,String> params, 
        String masterIp, int masterPort )
    {
        super( path, params, masterIp, masterPort );
    }
    
    public GraphDatabaseService getGraphDbService()
    {
        return super.getGraphDb();
    }
}
