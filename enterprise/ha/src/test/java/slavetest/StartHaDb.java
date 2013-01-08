/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package slavetest;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;
import org.neo4j.shell.ShellSettings;

public class StartHaDb
{
    public static final File PATH = new File( "var/hadb" );

    static final String ME = "172.16.1.242:5559";
    static final int MY_MACHINE_ID = 2;
        
    static final String[] ZOO_KEEPER_SERVERS = new String[] {
        "172.16.2.33:2181",
        "172.16.1.242:2181",
        "172.16.4.14:2181",
    };

    public static void main( String[] args ) throws Exception
    {
        NeoStoreUtil store = new NeoStoreUtil( PATH.getPath() );
        System.out.println( "Starting store: createTime=" + new Date( store.getCreationTime() ) +
                " identifier=" + store.getStoreId() + " last committed tx=" + store.getLastCommittedTx() );
        GraphDatabaseService db = startDb();
        System.out.println( "Waiting for ENTER (for clean shutdown)" );
        System.in.read();
        db.shutdown();
//        doStuff( db );
    }

    private static GraphDatabaseService startDb() throws IOException
    {
        return new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( PATH.getPath() ).
            setConfig( HaSettings.server_id, ""+MY_MACHINE_ID ).
            setConfig( HaSettings.coordinators, join( ZOO_KEEPER_SERVERS, "," ) ).
            setConfig( HaSettings.server, ME ).
            setConfig( ShellSettings.remote_shell_enabled, GraphDatabaseSetting.TRUE ).
            setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).
            newGraphDatabase();
    }
    
//    private static void doStuff( GraphDatabaseService db ) throws IOException
//    {
//        RelationshipType refType = DynamicRelationshipType.withName( "MATTIAS_REF" );
//        RelationshipType type = DynamicRelationshipType.withName( "JOHAN_IS_NOOB" );
//        long time = System.currentTimeMillis();
//        int txCount = 0;
//        while ( System.currentTimeMillis() - time < 60000 )
//        {
//            Transaction tx = db.beginTx();
//            boolean restarted = false;
//            try
//            {
//                Node refNode = db.getReferenceNode();
//                refNode.setProperty( "name", "MP" + System.currentTimeMillis() );
//                Node myRefNode = db.createNode();
//                refNode.createRelationshipTo( myRefNode, refType );
//                for ( int i = 0; i < 20; i++ )
//                {
//                    Node node = db.createNode();
//                    Relationship rel = myRefNode.createRelationshipTo( node, type );
//                    rel.setProperty( "something", i );
//                }
//                tx.success();
//
//                if ( Math.random() < 0.33 )
//                {
//                    db.shutdown();
//                    db = startDb();
//                    restarted = true;
//                }
//            }
//            finally
//            {
//                if ( !restarted )
//                {
//                    tx.finish();
//                }
//            }
//            verifyDb( db );
//
//            if ( ++txCount % 100 == 0 ) System.out.println( txCount );
//        }
//        System.out.println( "done " + txCount );
//    }
//
//    private static void verifyDb( GraphDatabaseService db )
//    {
//        for ( Node node : db.getAllNodes() )
//        {
//            for ( String key : node.getPropertyKeys() )
//            {
//                node.getProperty( key );
//            }
//            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
//            {
//                for ( String key : rel.getPropertyKeys() )
//                {
//                    rel.getProperty( key );
//                }
//            }
//        }
//    }

    public static String join( String[] strings, String delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( String string : strings )
        {
            builder.append( (builder.length() > 0 ? delimiter : "") + string );
        }
        return builder.toString();
    }
}
