/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.backup;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.util.Map;

import org.neo4j.com.ComException;
import org.neo4j.com.backup.OnlineBackup;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Get backup from an {@link EmbeddedGraphDatabase} running in a non-HA mode. 
 */
public class BackupFromEmbeddedDb
{
    public static void main( String[] args )
    {
        Args arguments = new Args( args );
        Number port = arguments.getNumber( "port", null );
        String from = arguments.get( "from", "localhost" );
        String target = getTarget( arguments );
        
        doBackup( target, from, port );
    }

    static String getTarget( Args arguments )
    {
        String target = arguments.get( "target", null );
        if ( target == null )
        {
            System.out.println( "You need to supply -target=<target directory>" );
            System.exit( 1 );
        }
        return target;
    }

    static void doBackup( String target, String from, Number portOrNull )
    {
        OnlineBackup backup = portOrNull == null ? OnlineBackup.from( from ) : OnlineBackup.from( from, portOrNull.intValue() );
        String fromMessage = from + (portOrNull != null ? ":" + backup.getPort() : "");
        try
        {
            if ( targetExists( target ) )
            {
                System.out.println( "Doing incremental backup from '" + fromMessage + "'" );
                backup.incremental( target );
            }
            else
            {
                System.out.println( "Doing full backup from '" + fromMessage + "'" );
                backup.full( target );
            }
            
            // TODO Verify contents in some way
            verifyContents( target );
            
            printCompletionMessage( backup );
        }
        catch ( ComException e )
        {
            System.out.println( "Couldn't connect to '" + fromMessage + "': " + e.getMessage() );
        }
        finally
        {
            try
            {
                backup.close();
            }
            catch ( Exception e )
            {
                // Couldn't shutdown... I don't think people would like to know about that.
                System.out.println( e.toString() );
            }
        }
    }

    private static void verifyContents( String target )
    {
        // TODO Progress meter?
        System.out.println( "Verifying contents" );
        GraphDatabaseService db = new EmbeddedGraphDatabase( target, stringMap( "enable_online_backup", "false" ) );
        try
        {
            for ( Node node : db.getAllNodes() )
            {
                verifyProperties( node );
                for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
                {
                    verifyProperties( rel );
                }
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void verifyProperties( PropertyContainer entity )
    {
        for ( String key : entity.getPropertyKeys() )
        {
            entity.getProperty( key );
        }
    }

    private static void printCompletionMessage( OnlineBackup backup )
    {
        System.out.println( "Backup completed, now at:" );
        for ( Map.Entry<String, Long> entry : backup.getLastCommittedTxs().entrySet() )
        {
            System.out.println( "  " + entry.getKey() + ": " + entry.getValue() );
        }
    }

    private static boolean targetExists( String target )
    {
        return new File( target, "neostore" ).exists();
    }
}
