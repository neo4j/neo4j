/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

public class Foo
{
    public static void main( String[] args )
    {
        new Foo().run();
    }

    private void run()
    {
        File dir = TargetDirectory.forTest( getClass() ).cleanDirectory( "test" );

        HighlyAvailableGraphDatabase db1 = startDb( 1, dir, "127.0.0.1:5001" );
        HighlyAvailableGraphDatabase db2 = startDb( 2, dir, "127.0.0.1:5001" );

        Transaction tx = db2.beginTx();
        db2.createNode();
        tx.success(); tx.finish();

        db2.shutdown();

        tx = db1.beginTx();
        db1.createNode();
        tx.success(); tx.finish();

        db1.shutdown();

        db2 = startDb( 2, dir, "127.0.0.1:5002" );

        tx = db2.beginTx();
        db2.createNode();
        tx.success(); tx.finish();

        tx = db2.beginTx();
        db2.createNode();
        tx.success(); tx.finish();

        db1 = startDb( 1, dir, "127.0.0.1:5002" );

        boolean success = false;

        while ( !success )
        {
            try
            {
                tx = db1.beginTx();
                db1.createNode();
                tx.success();
                success = true;
            }
            catch( Exception e )
            {
                tx.failure();
            }
            finally
            {
                tx.finish();
            }
        }

        db1.shutdown();
        db2.shutdown();

        System.out.println("-----> Done");
    }

    private static HighlyAvailableGraphDatabase startDb( int serverId, File path, String connectString )
    {
        GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( path, serverId ) )
                .setConfig( ClusterSettings.initial_hosts, connectString )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + serverId - 1 ) )
                .setConfig( ClusterSettings.server_id, "" + serverId )
                .setConfig( HaSettings.ha_server, ":" + (8001 + serverId) )
                .setConfig( HaSettings.tx_push_factor, "0" );
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) builder.newGraphDatabase();
        Transaction tx = db.beginTx();
        tx.finish();
        try
        {
            Thread.sleep( 2000 );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        return db;
    }

    private static String path( File path, int i )
    {
        return new File( path, "" + i ).getAbsolutePath();
    }
}
