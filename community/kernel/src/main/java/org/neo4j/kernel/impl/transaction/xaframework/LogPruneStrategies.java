/**
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.LogLoader;

import static org.neo4j.kernel.configuration.Config.parseLongWithUnit;

public class LogPruneStrategies
{
    public static final LogPruneStrategy NO_PRUNING = new LogPruneStrategy()
    {
        @Override
        public void prune( LogLoader source )
        {   // Don't prune logs at all.
        }
        
        @Override
        public String toString()
        {
            return "NO_PRUNING";
        }
    };
    
    static interface Threshold
    {
        boolean reached( File file, long version, LogLoader source );
    }
    
    private abstract static class AbstractPruneStrategy implements LogPruneStrategy
    {
        protected final FileSystemAbstraction fileSystem;

        AbstractPruneStrategy( FileSystemAbstraction fileSystem )
        {
            this.fileSystem = fileSystem;
        }
        
        @Override
        public void prune( LogLoader source )
        {
            if ( source.getHighestLogVersion() == 0 )
                return;
            
            long upper = source.getHighestLogVersion()-1;
            Threshold threshold = newThreshold();
            boolean exceeded = false;
            while ( upper >= 0 )
            {
                File file = source.getFileName( upper );
                if ( !fileSystem.fileExists( file ) )
                    // There aren't logs to prune anything. Just return
                    return;
                
                if ( fileSystem.getFileSize( file ) > LogIoUtils.LOG_HEADER_SIZE &&
                        threshold.reached( file, upper, source ) )
                {
                    exceeded = true;
                    break;
                }
                upper--;
            }
            
            if ( !exceeded )
                return;
            
            // Find out which log is the earliest existing (lower bound to prune)
            long lower = upper;
            while ( fileSystem.fileExists( source.getFileName( lower-1 ) ) )
                lower--;
            
            // The reason we delete from lower to upper is that if it crashes in the middle
            // we can be sure that no holes are created
            for ( long version = lower; version < upper; version++ )
                fileSystem.deleteFile( source.getFileName( version ) );
        }

        /**
         * @return a {@link Threshold} which if returning {@code false} states that the log file
         * is within the threshold and doesn't need to be pruned. The first time it returns
         * {@code true} it says that the threshold has been reached and the log file it just
         * returned {@code true} for should be kept, but all previous logs should be pruned.
         */
        protected abstract Threshold newThreshold();
    }
    
    public static LogPruneStrategy nonEmptyFileCount( FileSystemAbstraction fileSystem, int maxLogCountToKeep )
    {
        return new FileCountPruneStrategy( fileSystem, maxLogCountToKeep );
    }
    
    private static class FileCountPruneStrategy extends AbstractPruneStrategy
    {
        private final int maxNonEmptyLogCount;

        public FileCountPruneStrategy( FileSystemAbstraction fileSystem, int maxNonEmptyLogCount )
        {
            super( fileSystem );
            this.maxNonEmptyLogCount = maxNonEmptyLogCount;
        }
        
        @Override
        protected Threshold newThreshold()
        {
            return new Threshold()
            {
                int nonEmptyLogCount = 0;
                
                @Override
                public boolean reached( File file, long version, LogLoader source )
                {
                    return ++nonEmptyLogCount >= maxNonEmptyLogCount;
                }
            };
        }
        
        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[max:" + maxNonEmptyLogCount + "]";
        }
    }
    
    public static LogPruneStrategy totalFileSize( FileSystemAbstraction fileSystem, int numberOfBytes )
    {
        return new FileSizePruneStrategy( fileSystem, numberOfBytes );
    }
    
    public static class FileSizePruneStrategy extends AbstractPruneStrategy
    {
        private final int maxSize;

        public FileSizePruneStrategy( FileSystemAbstraction fileystem, int maxSizeBytes )
        {
            super( fileystem );
            this.maxSize = maxSizeBytes;
        }

        @Override
        protected Threshold newThreshold()
        {
            return new Threshold()
            {
                private int size;
                
                @Override
                public boolean reached( File file, long version, LogLoader source )
                {
                    size += fileSystem.getFileSize( file );
                    return size >= maxSize;
                }
            };
        }
    }
    
    public static LogPruneStrategy transactionCount( FileSystemAbstraction fileSystem, int maxCount )
    {
        return new TransactionCountPruneStrategy( fileSystem, maxCount );
    }
    
    public static class TransactionCountPruneStrategy extends AbstractPruneStrategy
    {
        private final int maxTransactionCount;

        public TransactionCountPruneStrategy( FileSystemAbstraction fileSystem, int maxTransactionCount )
        {
            super( fileSystem );
            this.maxTransactionCount = maxTransactionCount;
        }

        @Override
        protected Threshold newThreshold()
        {
            return new Threshold()
            {
                private Long highest;
                
                @Override
                public boolean reached( File file, long version, LogLoader source )
                {
                    try
                    {
                        // Here we know that the log version exists (checked in AbstractPruneStrategy#prune)
                        long tx = source.getFirstCommittedTxId( version );
                        if ( highest == null )
                        {
                            highest = source.getLastCommittedTxId();
                            return false;
                        }
                        return highest - tx >= maxTransactionCount;
                    }
                    catch(RuntimeException e)
                    {
                        if(e.getCause() != null && e.getCause() instanceof IllegalLogFormatException)
                        {
                            return decidePruneForIllegalLogFormat( (IllegalLogFormatException) e.getCause() );
                        }
                        else
                        {
                            throw e;
                        }
                    }
                }
            };
        }
    }
    
    public static LogPruneStrategy transactionTimeSpan( FileSystemAbstraction fileSystem, int timeToKeep, TimeUnit timeUnit )
    {
        return new TransactionTimeSpanPruneStrategy( fileSystem, timeToKeep, timeUnit );
    }
    
    public static class TransactionTimeSpanPruneStrategy extends AbstractPruneStrategy
    {
        private final int timeToKeep;
        private final TimeUnit unit;

        public TransactionTimeSpanPruneStrategy( FileSystemAbstraction fileSystem, int timeToKeep, TimeUnit unit )
        {
            super( fileSystem );
            this.timeToKeep = timeToKeep;
            this.unit = unit;
        }

        @Override
        protected Threshold newThreshold()
        {
            return new Threshold()
            {
                private long lowerLimit = System.currentTimeMillis() - unit.toMillis( timeToKeep );
                
                @Override
                public boolean reached( File file, long version, LogLoader source )
                {
                    try
                    {
                        return source.getFirstStartRecordTimestamp( version ) < lowerLimit;
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
            };
        }
    }

    private static boolean decidePruneForIllegalLogFormat( IllegalLogFormatException e )
    {
        if(e.wasNewerLogVersion())
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
    public static LogPruneStrategy fromConfigValue( FileSystemAbstraction fileSystem, String configValue )
    {
        String[] tokens = configValue.split( " " );
        if ( tokens.length == 0 )
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue + "'" );
        
        String numberWithUnit = tokens[0];
        if ( tokens.length == 1 )
        {
            if ( numberWithUnit.equals( "true" ) )
                return NO_PRUNING;
            else if ( numberWithUnit.equals( "false" ) )
                return transactionCount( fileSystem, 1 );
            else
                throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                        "'. The form is 'all' or '<number><unit> <type>' for example '100k txs' " +
                        "for the latest 100 000 transactions" );
        }
        
        String[] types = new String[] { "files", "size", "txs", "hours", "days" };
        String type = tokens[1];
        int number = (int) parseLongWithUnit( numberWithUnit );
        int typeIndex = 0;
        if ( type.equals( types[typeIndex++] ) )
            return nonEmptyFileCount( fileSystem, number );
        else if ( type.equals( types[typeIndex++] ) )
            return totalFileSize( fileSystem, number );
        else if ( type.equals( types[typeIndex++] ) )
            return transactionCount( fileSystem, number );
        else if ( type.equals( types[typeIndex++] ) )
            return transactionTimeSpan( fileSystem, number, TimeUnit.HOURS );
        else if ( type.equals( types[typeIndex++] ) )
            return transactionTimeSpan( fileSystem, number, TimeUnit.DAYS );
        else
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                    "'. Invalid type '" + type + "', valid are " + Arrays.asList( types ) );
    }
}
