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
package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogFiles;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class Check
{
    public static void main( String... args )
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        args = params.orphans().toArray( new String[0] );
        if ( args.length != 1 )
        {
            printUsage();
            System.exit( -1 );
            return;
        }
        String storeDir = args[0];
        if ( !new File( storeDir ).isDirectory() )
        {
            printUsage( String.format( "'%s' is not a directory", storeDir ) );
            System.exit( -1 );
        }
        if ( params.getBoolean( "recovery", false, true ) )
        {
            new EmbeddedGraphDatabase( storeDir ).shutdown();
        }
        else
        {
            XaLogicalLogFiles logFiles = new XaLogicalLogFiles(
                    new File( storeDir, NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME ).getAbsolutePath(),
                    new DefaultFileSystemAbstraction() );
            try
            {
                switch ( logFiles.determineState() )
                {
                case LEGACY_WITHOUT_LOG_ROTATION:
                    printUsage( "WARNING: store contains log file from too old version." );
                    break;
                case NO_ACTIVE_FILE:
                case CLEAN:
                    break;
                default:
                    printUsage( "Active logical log detected, this might be a source of inconsistencies.",
                                "Consider allowing the database to recover before running the consistency check.",
                                "Consistency checking will continue, abort if you wish to perform recovery first.",
                                "To perform recovery before checking consistency, use the '--recovery' flag." );
                }
            }
            catch ( IOException e )
            {
                System.err.printf( "Failure when checking for active logs: '%s', continuing as normal.%n", e );
            }
        }
        try
        {
            FullCheck.run( ProgressMonitorFactory.textual( System.err ), storeDir,
                           new Config( new ConfigurationDefaults( GraphDatabaseSettings.class ).apply(
                                   stringMap( FullCheck.consistency_check_property_owners.name(),
                                              Boolean.toString( params.getBoolean( "propowner", false, true ) ) ) ) ),
                           StringLogger.SYSTEM );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            e.printStackTrace( System.err );
        }
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines )
        {
            System.err.println( line );
        }
        System.err.println( Args.jarUsage( FullCheck.class, "[-propowner] [-recovery] <storedir>" ) );
        System.err.println( "WHERE:   <storedir>  is the path to the store to check" );
        System.err.println( "         -propowner  --  to verify that properties are owned only once" );
        System.err.println( "         -recovery   --  to perform recovery on the store before checking" );
    }

    private Check()
    {
        throw new UnsupportedOperationException( "No instances." );
    }
}
