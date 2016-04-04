/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.Args.jarUsage;
import static org.neo4j.helpers.Strings.joinAsLines;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConsistencyCheckTool
{
    private static final String CONFIG = "config";
    private static final String PROP_OWNER = "propowner";
    private static final String VERBOSE = "v";

    public static void main( String[] args ) throws IOException
    {
        ConsistencyCheckTool tool = new ConsistencyCheckTool( new ConsistencyCheckService(),
                new DefaultFileSystemAbstraction(), System.err );
        try
        {
            tool.run( args );
        }
        catch ( ToolFailureException e )
        {
            e.exitTool();
        }
    }

    private final ConsistencyCheckService consistencyCheckService;
    private final PrintStream systemError;
    private final FileSystemAbstraction fs;

    ConsistencyCheckTool( ConsistencyCheckService consistencyCheckService, FileSystemAbstraction fs,
            PrintStream systemError )
    {
        this.consistencyCheckService = consistencyCheckService;
        this.fs = fs;
        this.systemError = systemError;
    }

    void run( String... args ) throws ToolFailureException, IOException
    {
        Args arguments = Args.withFlags( PROP_OWNER, VERBOSE ).parse( args );

        File storeDir = determineStoreDirectory( arguments );
        Config tuningConfiguration = readConfiguration( arguments );
        boolean verbose = isVerbose( arguments );

        checkDbState( storeDir, tuningConfiguration );

        LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        try
        {
            consistencyCheckService.runFullConsistencyCheck( storeDir, tuningConfiguration,
                    ProgressMonitorFactory.textual( System.err ), logProvider, fs, verbose );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new ToolFailureException( "Check aborted due to exception", e );
        }
    }

    private boolean isVerbose( Args arguments )
    {
        return arguments.getBoolean( VERBOSE, false, true );
    }

    private void checkDbState( File storeDir, Config tuningConfiguration )
    {
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, tuningConfiguration ) )
        {
            if ( new RecoveryRequiredChecker( fs, pageCache ).isRecoveryRequiredAt( storeDir ) )
            {
                systemError.print( Strings.joinAsLines(
                        "Active logical log detected, this might be a source of inconsistencies.",
                        "Please recover database before running the consistency check.",
                        "To perform recovery please start database and perform clean shutdown." ) );

                exit();
            }
        }
        catch ( IOException e )
        {
            systemError.printf( "Failure when checking for recovery state: '%s', continuing as normal.%n", e );
        }
    }

    private File determineStoreDirectory( Args arguments ) throws ToolFailureException
    {
        List<String> unprefixedArguments = arguments.orphans();
        if ( unprefixedArguments.size() != 1 )
        {
            throw new ToolFailureException( usage() );
        }
        File storeDir = new File( unprefixedArguments.get( 0 ) );
        if ( !storeDir.isDirectory() )
        {
            throw new ToolFailureException(
                    Strings.joinAsLines( String.format( "'%s' is not a directory", storeDir ) ) + usage() );
        }
        return storeDir;
    }

    private Config readConfiguration( Args arguments ) throws ToolFailureException
    {
        Map<String,String> specifiedConfig = stringMap();

        String configFilePath = arguments.get( CONFIG, null );
        if ( configFilePath != null )
        {
            File configFile = new File( configFilePath );
            try
            {
                specifiedConfig = MapUtil.load( configFile );
            }
            catch ( IOException e )
            {
                throw new ToolFailureException( String.format( "Could not read configuration file [%s]",
                        configFilePath ), e );
            }
        }
        return new Config( specifiedConfig, GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }

    private String usage()
    {
        return joinAsLines(
                jarUsage( getClass(), "[-propowner] [-config <neo4j.conf>] [-v] <storedir>" ),
                "WHERE:   -propowner          also check property owner consistency (more time consuming)",
                "         -config <filename>  is the location of an optional properties file",
                "                             containing tuning parameters for the consistency check",
                "         -v                  produce execution output",
                "         <storedir>          is the path to the store to check"
        );
    }

    class ToolFailureException extends Exception
    {
        ToolFailureException( String message )
        {
            super( message );
        }

        ToolFailureException( String message, Throwable cause )
        {
            super( message, cause );
        }

        void exitTool()
        {
            System.err.println( getMessage() );
            if ( getCause() != null )
            {
                getCause().printStackTrace( System.err );
            }

            exit();
        }
    }

    private static void exit()
    {
        System.exit( 1 );
    }
}
