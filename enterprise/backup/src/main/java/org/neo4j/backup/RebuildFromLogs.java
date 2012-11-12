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

import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.getHighestHistoryLogVersion;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

class RebuildFromLogs
{
    private final XaDataSource nioneo;
    private final StoreAccess stores;

    RebuildFromLogs( AbstractGraphDatabase graphdb )
    {
        this.nioneo = getDataSource( graphdb, Config.DEFAULT_DATA_SOURCE_NAME );
        this.stores = new StoreAccess( graphdb );
    }

    RebuildFromLogs applyTransactionsFrom( File sourceDir ) throws IOException
    {
        LogExtractor extractor = null;
        try
        {
            extractor = LogExtractor.from( sourceDir.getAbsolutePath(), 2 );
            for ( InMemoryLogBuffer buffer = new InMemoryLogBuffer();; buffer.reset() )
            {
                long txId = extractor.extractNext( buffer );
                if ( txId == -1 ) break;
                applyTransaction( txId, buffer );
            }
        }
        finally
        {
            if ( extractor != null ) extractor.close();
        }
        return this;
    }

    public void applyTransaction( long txId, ReadableByteChannel txData ) throws IOException
    {
        nioneo.applyCommittedTransaction( txId, txData );
    }

    private static XaDataSource getDataSource( AbstractGraphDatabase graphdb, String name )
    {
        XaDataSource datasource = graphdb.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource( name );
        if ( datasource == null ) throw new NullPointerException( "Could not access " + name );
        return datasource;
    }

    public static void main( String[] args )
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        boolean full = params.getBoolean( "full", false, true );
        args = params.orphans().toArray( new String[0] );
        if ( args.length != 2 )
        {
            printUsage( "Exactly two positional arguments expected: <source dir with logs> <target dir for graphdb>" );
            System.exit( -1 );
            return;
        }
        File source = new File( args[0] ), target = new File( args[1] );
        if ( !source.isDirectory() )
        {
            printUsage( source + " is not a directory" );
            System.exit( -1 );
            return;
        }
        if ( target.exists() )
        {
            if ( target.isDirectory() )
            {
                if ( OnlineBackup.directoryContainsDb( target.getAbsolutePath() ) )
                {
                    printUsage( "target graph database already exists" );
                    System.exit( -1 );
                    return;
                }
                else
                {
                    System.err.println( "WARNING: the directory " + target + " already exists" );
                }
            }
            else
            {
                printUsage( target + " is a file" );
                System.exit( -1 );
                return;
            }
        }
        if ( findMaxLogFileId( source ) < 0 )
        {
            printUsage( "Inconsistent number of log files found in " + source );
            System.exit( -1 );
            return;
        }
        AbstractGraphDatabase graphdb = OnlineBackup.startTemporaryDb(
                target.getAbsolutePath(), full ? VerificationLevel.FULL_WITH_LOGGING : VerificationLevel.LOGGING );
        try
        {
            try
            {
                new RebuildFromLogs( graphdb ).applyTransactionsFrom( source ).fullCheck(!full);
            }
            finally
            {
                graphdb.shutdown();
            }
        }
        catch ( IOException e )
        {
            System.err.println();
            e.printStackTrace( System.err );
            System.exit( -1 );
            return;
        }
    }

    private void fullCheck( boolean full )
    {
        if ( full )
        {
            try
            {
                ConsistencyCheck.run( stores, true );
            }
            catch ( AssertionError summary )
            {
                System.err.println( summary.getMessage() );
            }
        }
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines ) System.err.println( line );
        System.err.println( Args.jarUsage( RebuildFromLogs.class, "[-full] <source dir with logs> <target dir for graphdb>" ) );
        System.err.println( "WHERE:   <source dir>  is the path for where transactions to rebuild from are stored" );
        System.err.println( "         <target dir>  is the path for where to create the new graph database" );
        System.err.println( "         -full     --  to run a full check over the entire store for each transaction" );
    }

    private static long findMaxLogFileId( File source )
    {
        return getHighestHistoryLogVersion( source, LOGICAL_LOG_DEFAULT_NAME );
    }
}
