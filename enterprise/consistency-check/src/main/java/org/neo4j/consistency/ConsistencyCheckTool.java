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
package org.neo4j.consistency;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogFiles;
import org.neo4j.kernel.impl.util.StringLogger;

public class ConsistencyCheckTool
{
    private static final String RECOVERY = "recovery";
    private static final String CONFIG = "config";

    public static void main( String[] args )
    {
        ConsistencyCheckTool tool = new ConsistencyCheckTool( new ConsistencyCheckService(), System.err );
        try
        {
            tool.run( args );
        }
        catch ( ToolFailureException e )
        {
            e.haltJVM();
        }
    }

    private final ConsistencyCheckService consistencyCheckService;
    private final PrintStream systemError;

    ConsistencyCheckTool( ConsistencyCheckService consistencyCheckService, PrintStream systemError )
    {
        this.consistencyCheckService = consistencyCheckService;
        this.systemError = systemError;
    }

    void run( String... args ) throws ToolFailureException
    {
        Args arguments = new Args( args );
        String storeDir = determineStoreDirectory( arguments );
        Config tuningConfiguration = readTuningConfiguration( storeDir, arguments );

        attemptRecoveryOrCheckStateOfLogicalLogs( arguments, storeDir );

        StringLogger logger = StringLogger.SYSTEM;
        try
        {
            consistencyCheckService.runFullConsistencyCheck( storeDir, tuningConfiguration,
                    ProgressMonitorFactory.textual( System.err ), logger );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new ToolFailureException( "Check aborted due to exception", e );
        }
        finally
        {
            logger.flush();
        }
    }

    private void attemptRecoveryOrCheckStateOfLogicalLogs( Args arguments, String storeDir )
    {
        if ( arguments.getBoolean( RECOVERY, false, true ) )
        {
            new GraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
        }
        else
        {
            XaLogicalLogFiles logFiles = new XaLogicalLogFiles(
                    new File( storeDir, NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME ),
                    new DefaultFileSystemAbstraction() );
            try
            {
                switch ( logFiles.determineState() )
                {
                    case LEGACY_WITHOUT_LOG_ROTATION:
                        systemError.println( "WARNING: store contains log file from too old version." );
                        break;
                    case NO_ACTIVE_FILE:
                    case CLEAN:
                        break;
                    default:
                        systemError.print( lines(
                                "Active logical log detected, this might be a source of inconsistencies.",
                                "Consider allowing the database to recover before running the consistency check.",
                                "Consistency checking will continue, abort if you wish to perform recovery first.",
                                "To perform recovery before checking consistency, use the '--recovery' flag." )
                        );
                }
            }
            catch ( IOException e )
            {
                systemError.printf( "Failure when checking for active logs: '%s', continuing as normal.%n", e );
            }
        }
    }

    private String determineStoreDirectory( Args arguments ) throws ToolFailureException
    {
        List<String> unprefixedArguments = arguments.orphans();
        if ( unprefixedArguments.size() != 1 )
        {
            throw new ToolFailureException( usage() );
        }
        String storeDir = unprefixedArguments.get( 0 );
        if ( !new File( storeDir ).isDirectory() )
        {
            throw new ToolFailureException( lines( String.format( "'%s' is not a directory", storeDir ) ) + usage() );
        }
        return storeDir;
    }

    private Config readTuningConfiguration( String storeDir, Args arguments ) throws ToolFailureException
    {
        Map<String, String> specifiedProperties = stringMap();

        String propertyFilePath = arguments.get( CONFIG, null );
        if ( propertyFilePath != null )
        {
            File propertyFile = new File( propertyFilePath );
            try
            {
                specifiedProperties = MapUtil.load( propertyFile );
            }
            catch ( IOException e )
            {
                throw new ToolFailureException( String.format( "Could not read configuration properties file [%s]",
                        propertyFilePath ), e );
            }
        }
        specifiedProperties.put( GraphDatabaseSettings.store_dir.name(), storeDir );
        return new Config( specifiedProperties, GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }

    private String usage()
    {
        return lines(
                Args.jarUsage( getClass(), "[-propowner] [-recovery] [-config <neo4j.properties>] <storedir>" ),
                "WHERE:   <storedir>         is the path to the store to check",
                "         -recovery          to perform recovery on the store before checking",
                "         <neo4j.properties> is the location of an optional properties file",
                "                            containing tuning parameters for the consistency check"
        );
    }

    private static String lines( String... content )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : content )
        {
            result.append( line ).append( System.getProperty( "line.separator" ) );
        }
        return result.toString();
    }

    static class ToolFailureException extends Exception
    {
        ToolFailureException( String message )
        {
            super( message );
        }

        ToolFailureException( String message, Throwable cause )
        {
            super( message, cause );
        }

        void haltJVM()
        {
            System.err.println( getMessage() );
            if ( getCause() != null )
            {
                getCause().printStackTrace( System.err );
            }
            System.exit( 1 );
        }
    }
}
