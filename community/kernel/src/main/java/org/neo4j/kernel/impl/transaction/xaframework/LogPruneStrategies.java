/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.neo4j.kernel.configuration.Config.parseLongWithUnit;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.FileSystemAbstraction;

public class LogPruneStrategies
{
    public static final LogPruneStrategy NO_PRUNING = new LogPruneStrategy()
    {
        @Override
        public void prune()
        {
            // do nothing
        }

        @Override
        public String toString()
        {
            return "NO_PRUNING";
        }
    };

    static interface Threshold
    {
        boolean reached( File file, long version, LogFileInformation source );
    }

    public static class ThresholdBasedPruneStrategy implements LogPruneStrategy
    {
        private final FileSystemAbstraction fileSystem;
        private final LogFileInformation logFileInformation;
        private final PhysicalLogFiles files;
        private final LogVersionRepository versionRepo;
        private final Threshold threshold;

        ThresholdBasedPruneStrategy( FileSystemAbstraction fileSystem, LogFileInformation logFileInformation,
                                     PhysicalLogFiles files, LogVersionRepository versionRepo, Threshold threshold )
        {
            this.fileSystem = fileSystem;
            this.logFileInformation = logFileInformation;
            this.files = files;
            this.versionRepo = versionRepo;
            this.threshold = threshold;
        }

        @Override
        public void prune()
        {
            long currentLogVersion = versionRepo.getCurrentLogVersion();
            if ( currentLogVersion == 0 )
            {
                return;
            }

            long upper = currentLogVersion-1;
            boolean exceeded = false;
            while ( upper >= 0 )
            {
                File file = files.getVersionFileName( upper );
                if ( !fileSystem.fileExists( file ) )
                {
                    // There aren't logs to prune anything. Just return
                    return;
                }

                if ( fileSystem.getFileSize( file ) > VersionAwareLogEntryReader.LOG_HEADER_SIZE &&
                        threshold.reached( file, upper, logFileInformation ) )
                {
                    exceeded = true;
                    break;
                }
                upper--;
            }

            if ( !exceeded )
            {
                return;
            }

            // Find out which log is the earliest existing (lower bound to prune)
            long lower = upper;
            while ( fileSystem.fileExists( files.getVersionFileName( lower-1 ) ) )
            {
                lower--;
            }

            // The reason we delete from lower to upper is that if it crashes in the middle
            // we can be sure that no holes are created
            for ( long version = lower; version <= upper; version++ )
            {
                fileSystem.deleteFile( files.getVersionFileName( version ) );
            }
        }
    }

    private static final class FileCountThreshold implements Threshold
    {
        private int nonEmptyLogCount = 0;
        private final int maxNonEmptyLogCount;

        private FileCountThreshold( int maxNonEmptyLogCount )
        {
            this.maxNonEmptyLogCount = maxNonEmptyLogCount;
        }

        @Override
        public boolean reached( File file, long version, LogFileInformation source )
        {
            return ++nonEmptyLogCount >= maxNonEmptyLogCount;
        }

        @Override
        public String toString()
        {
            return "[max:" + maxNonEmptyLogCount + "]";
        }
    }

    private static final class FileSizeThreshold implements Threshold
    {
        private int size;
        private final FileSystemAbstraction fileSystem;
        private final int maxSize;

        private FileSizeThreshold( FileSystemAbstraction fileSystem, int maxSize )
        {
            this.fileSystem = fileSystem;
            this.maxSize = maxSize;
        }

        @Override
        public boolean reached( File file, long version, LogFileInformation source )
        {
            size += fileSystem.getFileSize( file );
            return size >= maxSize;
        }
    }

    private static final class TransactionCountThreshold implements Threshold
    {
        private Long highest;
        private final long maxTransactionCount;

        private TransactionCountThreshold( long maxTransactionCount )
        {
            this.maxTransactionCount = maxTransactionCount;
        }

        @Override
        public boolean reached( File file, long version, LogFileInformation source )
        {
            try
            {
                // Here we know that the log version exists (checked in AbstractPruneStrategy#prune)
                long tx = source.getFirstCommittedTxId( version );
                if ( highest == null )
                {
                    highest = source.getLastCommittedTxId();
                }
                return highest - tx >= maxTransactionCount;
            }
            catch ( IllegalLogFormatException e )
            {
                return decidePruneForIllegalLogFormat( (IllegalLogFormatException) e.getCause() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static final class TransactionTimespanThreshold implements Threshold
    {
        private final TimeUnit timeUnit;
        private final int timeToKeep;

        private TransactionTimespanThreshold( TimeUnit timeUnit, int timeToKeep )
        {
            this.timeUnit = timeUnit;
            this.timeToKeep = timeToKeep;
        }

        @Override
        public boolean reached( File file, long version, LogFileInformation source )
        {
            try
            {
                long lowerLimit = System.currentTimeMillis() - timeUnit.toMillis( timeToKeep );
                long firstStartRecordTimestamp = source.getFirstStartRecordTimestamp( version );
                return firstStartRecordTimestamp >= 0 &&
                       firstStartRecordTimestamp < lowerLimit;
            }
            catch(IllegalLogFormatException e)
            {
                return decidePruneForIllegalLogFormat( e );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static boolean decidePruneForIllegalLogFormat( IllegalLogFormatException e )
    {
        if( e.wasNewerLogVersion() )
        {
            throw new RuntimeException( "Unable to read database logs, because it contains" +
                    " logs from a newer version of Neo4j.", e );
        }

        // Hit an old version log, consider this out of date.
        return true;
    }

    /**
     * Parses a configuration value for log specifying log pruning. It has one of these forms:
     * <ul>
     *   <li>all</li>
     *   <li>[number][unit] [type]</li>
     * </ul>
     * For example:
     * <ul>
     *   <li>100M size - For keeping last 100 megabytes of log data</li>
     *   <li>20 pcs - For keeping last 20 non-empty log files</li>
     *   <li>7 days - For keeping last 7 days worth of log data</li>
     *   <li>1k hours - For keeping last 1000 hours worth of log data</li>
     * </ul>
     */
    public static LogPruneStrategy fromConfigValue( FileSystemAbstraction fileSystem,
                                                    LogFileInformation logFileInformation, PhysicalLogFiles files,
                                                    LogVersionRepository versionRepo, String configValue )
    {
        String[] tokens = configValue.split( " " );
        if ( tokens.length == 0 )
        {
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue + "'" );
        }

        String numberWithUnit = tokens[0];

        Threshold thresholdToUse;

        if ( tokens.length == 1 )
        {
            if ( numberWithUnit.equals( "true" ) )
            {
                return NO_PRUNING;
            }
            else if ( numberWithUnit.equals( "false" ) )
            {
                thresholdToUse = new TransactionCountThreshold( 1 );
            }
            else
            {
                throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                        "'. The form is 'all' or '<number><unit> <type>' for example '100k txs' " +
                        "for the latest 100 000 transactions" );
            }
        }

        String[] types = new String[] { "files", "size", "txs", "hours", "days" };
        String type = tokens[1];
        int number = (int) parseLongWithUnit( numberWithUnit );
        int typeIndex = 0;
        if ( type.equals( types[typeIndex++] ) )
        {
            thresholdToUse = new FileCountThreshold( number );
        }
        else if ( type.equals( types[typeIndex++] ) )
        {
            thresholdToUse = new FileSizeThreshold( fileSystem, number );
        }
        else if ( type.equals( types[typeIndex++] ) )
        {
            thresholdToUse = new TransactionCountThreshold( number );
        }
        else if ( type.equals( types[typeIndex++] ) )
        {
            thresholdToUse = new TransactionTimespanThreshold( TimeUnit.HOURS, number );
        }
        else if ( type.equals( types[typeIndex++] ) )
        {
            thresholdToUse = new TransactionTimespanThreshold( TimeUnit.DAYS, number );
        }
        else
        {
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                    "'. Invalid type '" + type + "', valid are " + Arrays.asList( types ) );
        }

        return new ThresholdBasedPruneStrategy( fileSystem, logFileInformation, files, versionRepo, thresholdToUse );
    }
}
