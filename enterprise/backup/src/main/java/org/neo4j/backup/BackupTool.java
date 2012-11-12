/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.com.ComException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.storemigration.StoreFiles;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;

public class BackupTool
{
    private static final String TO = "to";
    private static final String FROM = "from";
    private static final String INCREMENTAL = "incremental";
    private static final String FULL = "full";
    private static final String VERIFY = "verify";
    public static final String DEFAULT_SCHEME = "single";

    public static void main( String[] args )
    {
        Args arguments = new Args( args );

        checkArguments( arguments );

        boolean full = arguments.has( FULL );
        String from = arguments.get( FROM, null );
        String to = arguments.get( TO, null );
        boolean verify = arguments.getBoolean( VERIFY, true, true );
        URI backupURI = null;
        try
        {
            backupURI = new URI( from );
        }
        catch ( URISyntaxException e )
        {
            exitAbnormally( "Please properly specify a location to backup as a valid URI in the form <scheme>://<host>[:port], where scheme is the target database's running mode, eg ha" );
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
                exitAbnormally( String.format(
                        "%s was specified as a backup module but it was not found. Please make sure that the implementing service is on the classpath.",
                        module ) );
            }
        }
        if ( service != null )
        { // If in here, it means a module was loaded. Use it and substitute the
          // passed URI
            backupURI = service.resolve( backupURI, arguments );
        }
        doBackup( full, backupURI, to, verify );
    }

    private static void checkArguments( Args arguments )
    {

        boolean full = arguments.has( FULL );
        boolean incremental = arguments.has( INCREMENTAL );
        if ( full&incremental || !(full|incremental) )
        {
            exitAbnormally( "Specify either " + dash( FULL ) + " or "
                            + dash( INCREMENTAL ) );
        }

        if ( arguments.get( FROM, null ) == null )
        {
            exitAbnormally( "Please specify " + dash( FROM ) );
        }

        if ( arguments.get( TO, null ) == null )
        {
            exitAbnormally( "Specify target location with " + dash( TO )
                            + " <target-directory>" );
        }
    }

    private static void doBackup( boolean trueForFullFalseForIncremental,
            URI from, String to, boolean verify )
    {
        if ( trueForFullFalseForIncremental )
        {
            doBackupFull( from, to, verify );
        }
        else
        {
            doBackupIncremental( from, to, verify );
        }
        System.out.println( "Done" );
    }

    private static void doBackupFull( URI from, String to, boolean verify )
    {
        System.out.println( "Performing full backup from '" + from + "'" );
        OnlineBackup backup = newOnlineBackup( from );
        try
        {
            backup.full( to, verify );
        }
        catch ( ComException e )
        {
            exitAbnormally( "Couldn't connect to '" + from + "'", e );
        }
    }

    private static void doBackupIncremental( URI from, String to, boolean verify )
    {
        System.out.println( "Performing incremental backup from '" + from + "'" );
        OnlineBackup backup = newOnlineBackup( from );
        boolean failedBecauseOfStoreVersionMismatch = false;
        try
        {
            backup.incremental( to, verify );
        }
        catch ( TransactionFailureException e )
        {
            if ( e.getCause() instanceof UpgradeNotAllowedByConfigurationException )
            {
                failedBecauseOfStoreVersionMismatch = true;
            }
            else
            {
                exitAbnormally( "TransactionFailureException from existing backup at '" + from + "'.", e);
            }
        }
        catch ( ComException e )
        {
            exitAbnormally( "Couldn't connect to '" + from + "' ", e );
        }
        if ( failedBecauseOfStoreVersionMismatch )
        {
            System.out.println( "The database present in the target directory is of an older version. Backing that up in "
                        + "target"
                        + " and performing a full backup from source" );
            try
            {
                moveExistingDatabase( to );
            }
            catch ( IOException e )
            {
                exitAbnormally( "There was a problem moving the old database out of the way - cannot continue, aborting.", e );
            }
            doBackupFull( from, to, verify );
        }
    }

    private static void moveExistingDatabase( String to ) throws IOException
    {
        File toDir = new File(to);
        File backupDir = new File( toDir, "old-version" );
        if ( !backupDir.mkdir() )
        {
            throw new IOException( "Trouble making target backup directory "
                                   + backupDir.getAbsolutePath() );
        }
        StoreFiles.move( toDir, backupDir );
        LogFiles.move( toDir, backupDir );
    }

    private static void exitAbnormally( String message, Exception ex )
    {
        System.out.println( message );
        ex.printStackTrace( System.out );
        System.exit( 1 );
    }

    private static void exitAbnormally( String message )
    {
        System.out.println( message );
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
