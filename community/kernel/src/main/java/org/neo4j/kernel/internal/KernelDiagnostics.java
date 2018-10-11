/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.internal;

import java.io.File;
import java.io.FileFilter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.neo4j.helpers.Format;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.Logger;

public abstract class KernelDiagnostics implements DiagnosticsProvider
{
    public static class Versions extends KernelDiagnostics
    {
        private final DatabaseInfo databaseInfo;
        private final StoreId storeId;

        public Versions( DatabaseInfo databaseInfo, StoreId storeId )
        {
            this.databaseInfo = databaseInfo;
            this.storeId = storeId;
        }

        @Override
        void dump( Logger logger )
        {
            logger.log( "Graph Database: " + databaseInfo + " " + storeId );
            logger.log( "Kernel version: " + Version.getKernelVersion() );
        }
    }

    public static class StoreFiles extends KernelDiagnostics
    {
        private final File storeDir;
        private static String FORMAT_DATE_ISO = "yyyy-MM-dd'T'HH:mm:ssZ";
        private final SimpleDateFormat dateFormat;

        public StoreFiles( File storeDir )
        {
            this.storeDir = storeDir;
            TimeZone tz = TimeZone.getDefault();
            dateFormat = new SimpleDateFormat( FORMAT_DATE_ISO );
            dateFormat.setTimeZone( tz );
        }

        @Override
        void dump( Logger logger )
        {
            logger.log( getDiskSpace( storeDir ) );
            logger.log( "Storage files: (filename : modification date - size)" );
            MappedFileCounter mappedCounter = new MappedFileCounter( storeDir );
            long totalSize = logStoreFiles( logger, "  ", storeDir, mappedCounter );
            logger.log( "Storage summary: " );
            logger.log( "  Total size of store: " + Format.bytes( totalSize ) );
            logger.log( "  Total size of mapped files: " + Format.bytes( mappedCounter.getSize() ) );
        }

        private long logStoreFiles( Logger logger, String prefix, File dir, MappedFileCounter mappedCounter )
        {
            if ( !dir.isDirectory() )
            {
                return 0;
            }
            File[] files = dir.listFiles();
            if ( files == null )
            {
                logger.log( prefix + "<INACCESSIBLE>" );
                return 0;
            }
            long total = 0;

            // Sort by name
            List<File> fileList = Arrays.asList( files );
            fileList.sort( Comparator.comparing( File::getName ) );

            for ( File file : fileList )
            {
                long size;
                String filename = file.getName();
                if ( file.isDirectory() )
                {
                    logger.log( prefix + filename + ":" );
                    size = logStoreFiles( logger, prefix + "  ", file, mappedCounter );
                    filename = "- Total";
                }
                else
                {
                    size = file.length();
                    mappedCounter.addFile( file );
                }

                String fileModificationDate = getFileModificationDate( file );
                String bytes = Format.bytes( size );
                String fileInformation = String.format( "%s%s: %s - %s", prefix, filename, fileModificationDate, bytes );
                logger.log( fileInformation );

                total += size;
            }
            return total;
        }

        private String getFileModificationDate( File file )
        {
            Date modifiedDate = new Date( file.lastModified() );
            return dateFormat.format( modifiedDate );
        }

        private String getDiskSpace( File storeDir )
        {
            long free = storeDir.getFreeSpace();
            long total = storeDir.getTotalSpace();
            long percentage = total != 0 ? (free * 100 / total) : 0;
            return String.format( "Disk space on partition (Total / Free / Free %%): %s / %s / %s", total, free, percentage );
        }

        private static class MappedFileCounter
        {
            private final FileFilter mappedIndexFilter;
            private long size;

            MappedFileCounter( File storeDir )
            {
                mappedIndexFilter = new NativeIndexFileFilter( storeDir );
            }

            void addFile( File file )
            {
                if ( StoreType.canBeManagedByPageCache( file.getName() ) || mappedIndexFilter.accept( file ) )
                {
                    size += file.length();
                }
            }

            public long getSize()
            {
                return size;
            }
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
    public void dump( DiagnosticsPhase phase, Logger log )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            dump( log );
        }
    }

    abstract void dump( Logger logger );
}
