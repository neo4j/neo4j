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
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
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
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.DuplicatingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;

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

    public Result runFullConsistencyCheck( File storeDir,
                                           Config tuningConfiguration,
                                           ProgressMonitorFactory progressFactory,
                                           LogProvider logProvider ) throws ConsistencyCheckIncompleteException, IOException
    {
        Log log = logProvider.getLog( getClass() );
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, tuningConfiguration, PageCacheTracer.NULL, logProvider.getLog( PageCache.class ) );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        try
        {
            return runFullConsistencyCheck(
                    storeDir, tuningConfiguration, progressFactory, logProvider, fileSystem, pageCache );
        }
        finally
        {
            try
            {
                pageCache.close();
            }
            catch ( IOException e )
            {
                log.error( "Failure during shutdown of the page cache", e );
            }
        }
    }

    public Result runFullConsistencyCheck( File storeDir, Config tuningConfiguration,
                                           ProgressMonitorFactory progressFactory,
                                           LogProvider logProvider,
                                           final FileSystemAbstraction fileSystem,
                                           PageCache pageCache )
            throws ConsistencyCheckIncompleteException
    {
        Log log = logProvider.getLog( getClass() );
        Monitors monitors = new Monitors();
        StoreFactory factory = new StoreFactory(
                storeDir,
                tuningConfiguration,
                new DefaultIdGeneratorFactory(),
                pageCache, fileSystem, logProvider,
                monitors
        );

        ConsistencySummaryStatistics summary;
        final File reportFile = chooseReportPath( storeDir, tuningConfiguration );
        Log reportLog = new ConsistencyReportLog( Suppliers.lazySingleton( new Supplier<PrintWriter>()
        {
            @Override
            public PrintWriter get()
            {
                try
                {
                    return new PrintWriter( createOrOpenAsOuputStream( fileSystem, reportFile, true ) );
                } catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } ) );

        try ( NeoStore neoStore = factory.newNeoStore( false ) )
        {
            neoStore.makeStoreOk();
            StoreAccess store = new StoreAccess( neoStore );
            LabelScanStore labelScanStore = null;
            try
            {
                labelScanStore = new LuceneLabelScanStoreBuilder(
                        storeDir, store.getRawNeoStore(), fileSystem, logProvider ).build();
                SchemaIndexProvider indexes = new LuceneSchemaIndexProvider(
                        DirectoryFactory.PERSISTENT,
                        storeDir );
                DirectStoreAccess stores = new DirectStoreAccess( store, labelScanStore, indexes );
                FullCheck check = new FullCheck( tuningConfiguration, progressFactory );
                summary = check.execute( stores, new DuplicatingLog( log, reportLog ) );
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
                    log.error( "Failure during shutdown of label scan store", e );
                }
            }
        }

        if ( !summary.isConsistent() )
        {
            log.warn( "See '%s' for a detailed consistency report.", reportFile.getPath() );
            return Result.FAILURE;
        }

        return Result.SUCCESS;
    }

    private File chooseReportPath( File storeDir, Config tuningConfiguration )
    {
        final File reportPath = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_report_file );
        if ( reportPath == null )
        {
            return new File( storeDir, defaultLogFileName( timestamp ) );
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
