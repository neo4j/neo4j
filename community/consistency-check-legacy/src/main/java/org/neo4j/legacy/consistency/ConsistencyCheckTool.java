/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.legacy.consistency;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.legacy.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

import static org.neo4j.helpers.Args.jarUsage;
import static org.neo4j.helpers.Strings.joinAsLines;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConsistencyCheckTool
{
    private static final String RECOVERY = "recovery";
    private static final String CONFIG = "config";
    private static final String PROP_OWNER = "propowner";

    public interface ExitHandle
    {
        ExitHandle SYSTEM_EXIT = new ExitHandle()
        {
            @Override
            public void pull()
            {
                System.exit( 1 );
            }
        };

        void pull();
    }

    public static void main( String[] args ) throws IOException
    {
        ConsistencyCheckTool tool = new ConsistencyCheckTool( new ConsistencyCheckService(), new GraphDatabaseFactory(),
                new DefaultFileSystemAbstraction(), System.err, ExitHandle.SYSTEM_EXIT );
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
    private final GraphDatabaseFactory dbFactory;
    private final PrintStream systemError;
    private final ExitHandle exitHandle;
    private final FileSystemAbstraction fs;

    public ConsistencyCheckTool( ConsistencyCheckService consistencyCheckService,
            GraphDatabaseFactory dbFactory, FileSystemAbstraction fs, PrintStream systemError, ExitHandle exitHandle )
    {
        this.consistencyCheckService = consistencyCheckService;
        this.dbFactory = dbFactory;
        this.fs = fs;
        this.systemError = systemError;
        this.exitHandle = exitHandle;
    }

    public void run( String... args ) throws ToolFailureException, IOException
    {
        Args arguments = Args.withFlags( RECOVERY, PROP_OWNER ).parse( args );
        File storeDir = determineStoreDirectory( arguments );
        Config tuningConfiguration = readTuningConfiguration( arguments );

        attemptRecoveryOrCheckStateOfLogicalLogs( arguments, storeDir, tuningConfiguration );

        LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        try
        {
            consistencyCheckService.runFullConsistencyCheck( storeDir, tuningConfiguration,
                    ProgressMonitorFactory.textual( System.err ), logProvider, fs );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new ToolFailureException( "Check aborted due to exception", e );
        }
    }

    private void attemptRecoveryOrCheckStateOfLogicalLogs( Args arguments, File storeDir, Config tuningConfiguration )
    {
        if ( arguments.getBoolean( RECOVERY, false, true ) )
        {
            dbFactory.newEmbeddedDatabase( storeDir ).shutdown();
        }
        else
        {
            try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, tuningConfiguration ) )
            {
                if ( new RecoveryRequiredChecker( fs, pageCache ).isRecoveryRequiredAt( storeDir ) )
                {
                    systemError.print( joinAsLines(
                            "Active logical log detected, this might be a source of inconsistencies.",
                            "Consider allowing the database to recover before running the consistency check.",
                            "Consistency checking will continue, abort if you wish to perform recovery first.",
                            "To perform recovery before checking consistency, use the '--recovery' flag." ) );

                    exitHandle.pull();
                }
            }
            catch ( IOException e )
            {
                systemError.printf( "Failure when checking for recovery state: '%s', continuing as normal.%n", e );
            }
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
            throw new ToolFailureException( joinAsLines( format( "'%s' is not a directory", storeDir ) ) + usage() );
        }
        return storeDir;
    }

    private Config readTuningConfiguration( Args arguments ) throws ToolFailureException
    {
        Map<String,String> specifiedProperties = stringMap();

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
        return new Config( specifiedProperties, GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }

    private String usage()
    {
        return joinAsLines(
                jarUsage( getClass(), "[-propowner] [-recovery] [-config <neo4j.properties>] <storedir>" ),
                "WHERE:   <storedir>         is the path to the store to check",
                "         -recovery          to perform recovery on the store before checking",
                "         <neo4j.properties> is the location of an optional properties file",
                "                            containing tuning parameters for the consistency check"
        );
    }

    public class ToolFailureException extends Exception
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

            exitHandle.pull();
        }
    }
}
