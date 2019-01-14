/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ha;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class StartInstanceInAnotherJvm
{
    private StartInstanceInAnotherJvm()
    {
    }

    public static void main( String[] args )
    {
        File dir = new File( args[0] );
        GraphDatabaseAPI newSlave = (GraphDatabaseAPI) new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir )
                .setConfig( Args.parse( args ).asMap() )
                .newGraphDatabase();
    }

    public static Process start( String dir, Map<String, String> config ) throws Exception
    {
        List<String> args = new ArrayList<>( Arrays.asList( "java", "-cp", System.getProperty( "java.class.path" ),
                StartInstanceInAnotherJvm.class.getName(), dir ) );
        for ( Map.Entry<String,String> property : config.entrySet() )
        {
            args.add( "-" + property.getKey() + "=" + property.getValue() );
        }
        return Runtime.getRuntime().exec( args.toArray( new String[ args.size() ] ) );
    }
}
