/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.index.lucene.LuceneLabelScanStoreBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.store.StoreFactory.configForStoreDir;

public class ConsistencyCheckService
{
    private final Date timestamp;

    public ConsistencyCheckService()
    {
        this( new Date() );
    }

    public ConsistencyCheckService( Date timestamp )
    {
        this.timestamp = timestamp;
    }

    public Result runFullConsistencyCheck( String storeDir,
                                                  Config tuningConfiguration,
                                                  ProgressMonitorFactory progressFactory,
                                           StringLogger logger ) throws ConsistencyCheckIncompleteException
    {
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        tuningConfiguration = configForStoreDir( tuningConfiguration, new File( storeDir ) );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, tuningConfiguration, PageCacheTracer.NULL );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        try
        {
            return runFullConsistencyCheck(
                    storeDir, tuningConfiguration, progressFactory, logger, fileSystem, pageCache );
        }
        finally
        {
            try
            {
                pageCache.close();
            }
            catch ( IOException e )
            {
                logger.error( "Failure during shutdown of the page cache", e );
            }
        }
    }

    public Result runFullConsistencyCheck( String storeDir, Config tuningConfiguration,
                                           ProgressMonitorFactory progressFactory,
                                           StringLogger logger,
                                           FileSystemAbstraction fileSystem,
                                           PageCache pageCache )
            throws ConsistencyCheckIncompleteException
    {
        Monitors monitors = new Monitors();
        StoreFactory factory = new StoreFactory(
                tuningConfiguration,
                new DefaultIdGeneratorFactory(),
                pageCache, fileSystem, logger,
                monitors
        );

        ConsistencySummaryStatistics summary;
        File reportFile = chooseReportPath( tuningConfiguration );
        StringLogger report = StringLogger.lazyLogger( reportFile );

        try ( NeoStore neoStore = factory.newNeoStore( false ) )
        {
            neoStore.makeStoreOk();
            StoreAccess store = new StoreAccess( neoStore );
            LabelScanStore labelScanStore = null;
            try
            {

                labelScanStore = new LuceneLabelScanStoreBuilder(
                        storeDir, store.getRawNeoStore(), fileSystem, logger ).build();
                SchemaIndexProvider indexes = new LuceneSchemaIndexProvider(
                        DirectoryFactory.PERSISTENT,
                        tuningConfiguration );
                DirectStoreAccess stores = new DirectStoreAccess( store, labelScanStore, indexes );
                FullCheck check = new FullCheck( tuningConfiguration, progressFactory );
                summary = check.execute( stores, StringLogger.tee( logger, report ) );
            }
            finally
            {
                try
                {
                    if ( null != labelScanStore )
                    {
                        labelScanStore.shutdown();
                    }
                }
                catch ( IOException e )
                {
                    logger.error( "Failure during shutdown of label scan store", e );
                }
            }
        }
        finally
        {
            report.close();
        }

        if ( !summary.isConsistent() )
        {
            logger.logMessage( String.format( "See '%s' for a detailed consistency report.", reportFile.getPath() ) );
            return Result.FAILURE;
        }

        return Result.SUCCESS;
    }

    private File chooseReportPath( Config tuningConfiguration )
    {
        final File reportPath = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_report_file );
        if ( reportPath == null )
        {
            return new File( tuningConfiguration.get( GraphDatabaseSettings.store_dir ),
                    defaultLogFileName( timestamp ) );
        }

        if ( reportPath.isDirectory() )
        {
            return new File( reportPath, defaultLogFileName( timestamp ) );
        }

        return reportPath;
    }

    public static String defaultLogFileName( Date date )
    {
        final String formattedDate = new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( date );
        return String.format( "inconsistencies-%s.report", formattedDate );
    }

    public static enum Result
    {
        FAILURE( false ), SUCCESS( true );

        private final boolean successful;

        private Result( boolean successful )
        {
            this.successful = successful;
        }

        public boolean isSuccessful()
        {
            return this.successful;
        }
    }
}
