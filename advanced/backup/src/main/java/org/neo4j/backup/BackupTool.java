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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;

import org.neo4j.com.ComException;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;

public class BackupTool
{
    private static final String TO = "to";
    private static final String FROM = "from";
    private static final String INCREMENTAL = "incremental";
    private static final String FULL = "full";
    public static final String DEFAULT_SCHEME = "simple";

    public static void main( String[] args )
    {
        Args arguments = new Args( args );

        checkArguments( arguments );

        boolean full = arguments.has( FULL );
        String from = arguments.get( FROM, null );
        String to = arguments.get( TO, null );
        URI backupURI = null;
        try
        {
            backupURI = new URI( from );
        }
        catch ( URISyntaxException e )
        {
            System.out.println( "Please properly specify a location to backup as a valid URI in the form <scheme>://<host>[:port], where scheme is the target database's running mode, eg ha" );
            exitAbnormally();
        }
        String module = backupURI.getScheme();

        /*
         * So, the scheme is considered to be the module name and an attempt at
         * loading the service is made.
         */
        BackupExtensionService service = null;
        if ( module != null && !DEFAULT_SCHEME.equals( module ) )
        {
            try
            {
                service = Service.load( BackupExtensionService.class, module );
            }
            catch ( NoSuchElementException e )
            {
                System.out.println( String.format(
                        "%s was specified as a backup module but it was not found. Please make sure that the implementing service is on the classpath.",
                        module ) );
                exitAbnormally();
            }
        }
        if ( service != null )
        { // If in here, it means a module was loaded. Use it and substitute the
          // passed URI
            backupURI = service.resolve( backupURI );
        }
        doBackup( full, backupURI, to );
    }

    private static void checkArguments( Args arguments )
    {

        boolean full = arguments.has( FULL );
        boolean incremental = arguments.has( INCREMENTAL );
        if ( full&incremental || !(full|incremental) )
        {
            System.out.println( "Specify either " + dash( FULL ) + " or " + dash( INCREMENTAL ) );
            exitAbnormally();
        }

        if ( arguments.get( FROM, null ) == null )
        {
            System.out.println( "Please specify " + dash( FROM ) );
            exitAbnormally();
        }

        if ( arguments.get( TO, null ) == null )
        {
            System.out.println( "Specify target location with " + dash( TO ) + " <target-directory>" );
            exitAbnormally();
        }
    }

    private static void doBackup( boolean trueForFullFalseForIncremental,
            URI from, String to )
    {
        OnlineBackup backup = newOnlineBackup( from );
        try
        {
            if ( trueForFullFalseForIncremental )
            {
                System.out.println( "Performing full backup from '" + from + "'" );
                backup.full( to );
            }
            else
            {
                System.out.println( "Performing incremental backup from '" + from + "'" );
                backup.incremental( to );
            }
            System.out.println( "Done" );
        }
        catch ( ComException e )
        {
            System.out.println( "Couldn't connect to '" + from + "', " + e.getMessage() );
            exitAbnormally();
        }
    }

    private static void exitAbnormally()
    {
        System.exit( 1 );
    }

    private static String dash( String name )
    {
        return "-" + name;
    }

    private static OnlineBackup newOnlineBackup( URI from )
    {
        String host = from.getHost();
        int port = from.getPort();
        if ( port == -1 )
            return OnlineBackup.from( host );
        else
            return OnlineBackup.from( host, port );
    }
}
