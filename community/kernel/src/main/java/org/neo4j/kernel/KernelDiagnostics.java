/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;

abstract class KernelDiagnostics implements DiagnosticsProvider
{
    static void register( DiagnosticsManager manager, InternalAbstractGraphDatabase graphdb, NeoStoreXaDataSource ds )
    {
        manager.prependProvider( new Versions( graphdb.getClass(), ds ) );
        ds.registerDiagnosticsWith( manager );
        manager.appendProvider( new StoreFiles( graphdb.getStoreDir() ) );
    }

    private static class Versions extends KernelDiagnostics
    {
        private final Class<? extends GraphDatabaseService> graphDb;
        private final StoreId storeId;

        public Versions( Class<? extends GraphDatabaseService> graphDb, NeoStoreXaDataSource ds )
        {
            this.graphDb = graphDb;
            this.storeId = ds.getStoreId();
        }

        @Override
        void dump( StringLogger logger )
        {
            logger.logMessage( "Graph Database: " + graphDb.getName() + " " + storeId );
            logger.logMessage( "Kernel version: " + Version.getKernel() );
            logger.logMessage( "Neo4j component versions:" );
            for ( Version componentVersion : Service.load( Version.class ) )
            {
                logger.logMessage( "  " + componentVersion );
            }
        }
    }

    private static class StoreFiles extends KernelDiagnostics implements Visitor<StringLogger.LineLogger>
    {
        private final File storeDir;

        private StoreFiles( String storeDir )
        {
            this.storeDir = new File(storeDir);
        }

        @Override
        void dump( StringLogger logger )
        {
            logger.logLongMessage( "Storage files:", this, true );
        }

        @Override
        public boolean visit( StringLogger.LineLogger logger )
        {
            logStoreFiles( logger, "",  storeDir );
            return false;
        }

        private static long logStoreFiles( StringLogger.LineLogger logger, String prefix, File dir )
        {
            if ( !dir.isDirectory() ) return 0;
            File[] files = dir.listFiles();
            if ( files == null )
            {
                logger.logLine( prefix + "<INACCESSIBLE>" );
                return 0;
            }
            long total = 0;
            for ( File file : files )
            {
                long size;
                String filename = file.getName();
                if ( file.isDirectory() )
                {
                    logger.logLine( prefix + filename + ":" );
                    size = logStoreFiles( logger, prefix + "  ", file );
                    filename = "- Total";
                }
                else
                {
                    size = file.length();
                }
                logger.logLine( prefix + filename + ": " + Format.bytes( size ) );
                total += size;
            }
            return total;
        }
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getDeclaringClass().getSimpleName() + ":" + getClass().getSimpleName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // nothing visits ConfigurationLogging
    }

    @Override
    public void dump( DiagnosticsPhase phase, StringLogger log )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            dump( log );
        }
    }

    abstract void dump( StringLogger logger );
}
