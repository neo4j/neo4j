/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.ha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.GraphDatabaseAPI;

public class StartInstanceInAnotherJvm
{
    public static void main( String[] args )
    {
        String dir = args[0];
        GraphDatabaseAPI newSlave = (GraphDatabaseAPI) new TestHighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( dir )
                .setConfig( Args.parse( args ).asMap() )
                .newGraphDatabase();
    }

    public static Process start( String dir, Map<String, String> config ) throws Exception
    {
        List<String> args = new ArrayList<String>();
        args.addAll( Arrays.asList( "java", "-cp", System.getProperty( "java.class.path" ),
                StartInstanceInAnotherJvm.class.getName(), dir ) );
        for ( Map.Entry<String, String> property : config.entrySet() )
            args.add( "-" + property.getKey() + "=" + property.getValue() );
        return Runtime.getRuntime().exec( args.toArray( new String[ args.size() ] ) );
    }
}
