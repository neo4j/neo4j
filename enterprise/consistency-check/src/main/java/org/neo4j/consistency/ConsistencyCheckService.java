/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public class ConsistencyCheckService
{
    private final Date timestamp = new Date();

    public void runFullConsistencyCheck( String storeDir,
                                         Config tuningConfiguration,
                                         ProgressMonitorFactory progressFactory,
                                         StringLogger logger ) throws ConsistencyCheckIncompleteException
    {
        Map<String, String> params = tuningConfiguration.getParams();
        params.put( GraphDatabaseSettings.store_dir.name(), storeDir );
        tuningConfiguration.applyChanges( params );

        StoreFactory factory = new StoreFactory(
                tuningConfiguration,
                new DefaultIdGeneratorFactory(),
                tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_window_pool_implementation )
                        .windowPoolFactory( tuningConfiguration, logger ), new DefaultFileSystemAbstraction(), logger,
                new DefaultTxHook() );

        ConsistencySummaryStatistics summary;
        File reportFile = chooseReportPath( tuningConfiguration );
        StringLogger report = StringLogger.lazyLogger( reportFile );

        NeoStore neoStore = factory.newNeoStore( new File( storeDir, NeoStore.DEFAULT_NAME ) );
        try
        {
            neoStore.makeStoreOk();
            StoreAccess store = new StoreAccess( neoStore );
            summary = new FullCheck( tuningConfiguration, progressFactory )
                    .execute( store, StringLogger.tee( logger, report ) );
        }
        finally
        {
            neoStore.close();
        }

        if ( !summary.isConsistent() )
        {
            logger.logMessage( String.format( "See '%s' for a detailed consistency report.", reportFile.getPath() ) );
        }
    }

    private File chooseReportPath( Config tuningConfiguration )
    {
        File reportPath = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_report_file );
        File reportFile;
        if ( reportPath == null )
        {
            reportFile = new File( tuningConfiguration.get( GraphDatabaseSettings.store_dir ), defaultLogFileName() );
        } else
        {
            if ( reportPath.isDirectory() )
            {
                reportFile = new File( reportPath, defaultLogFileName() );
            } else
                reportFile = reportPath;
        }
        return reportFile;
    }

    String defaultLogFileName()
    {
        return String.format( "inconsistencies-%s.report",
                new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( timestamp ) );
    }
}
