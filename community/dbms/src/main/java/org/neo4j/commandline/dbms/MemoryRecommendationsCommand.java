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
package org.neo4j.commandline.dbms;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.util.Locale;
import java.util.function.ToDoubleFunction;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.api.impl.index.storage.FailureStorage;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.internal.NativeIndexFileFilter;

import static java.lang.String.format;
import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.configuration.ExternalSettings.initialHeapSize;
import static org.neo4j.configuration.ExternalSettings.maxHeapSize;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.active_database;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.io.ByteUnit.ONE_GIBI_BYTE;
import static org.neo4j.io.ByteUnit.ONE_KIBI_BYTE;
import static org.neo4j.io.ByteUnit.ONE_MEBI_BYTE;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.baseSchemaIndexFolder;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.buildSetting;

public class MemoryRecommendationsCommand implements AdminCommand
{
    // Fields: {System Memory in GiBs; OS memory reserve in GiBs; JVM Heap memory in GiBs}.
    // And the page cache gets what's left, though always at least 100 MiB.
    // Heap never goes beyond 31 GiBs.
    private static final Bracket[] datapoints = {
            new Bracket( 0.01, 0.005, 0.005 ),
            new Bracket( 1.0, 0.5, 0.5 ),
            new Bracket( 2.0, 1, 0.5 ),
            new Bracket( 4.0, 1.5, 2 ),
            new Bracket( 6.0, 2, 3 ),
            new Bracket( 8.0, 2.5, 3.5 ),
            new Bracket( 10.0, 3, 4 ),
            new Bracket( 12.0, 3.5, 4.5 ),
            new Bracket( 16.0, 4, 5 ),
            new Bracket( 24.0, 6, 8 ),
            new Bracket( 32.0, 8, 12 ),
            new Bracket( 64.0, 12, 24 ),
            new Bracket( 128.0, 16, 31 ),
            new Bracket( 256.0, 20, 31 ),
            new Bracket( 512.0, 24, 31 ),
            new Bracket( 1024.0, 30, 31 ),
    };
    private static final String ARG_MEMORY = "memory";
    private final Path homeDir;
    private final OutsideWorld outsideWorld;
    private final Path configDir;

    static long recommendOsMemory( long totalMemoryBytes )
    {
        Brackets brackets = findMemoryBrackets( totalMemoryBytes );
        return brackets.recommend( Bracket::osMemory );
    }

    static long recommendHeapMemory( long totalMemoryBytes )
    {
        Brackets brackets = findMemoryBrackets( totalMemoryBytes );
        return brackets.recommend( Bracket::heapMemory );
    }

    static long recommendPageCacheMemory( long totalMemoryBytes )
    {
        long osMemory = recommendOsMemory( totalMemoryBytes );
        long heapMemory = recommendHeapMemory( totalMemoryBytes );
        long recommendation = totalMemoryBytes - osMemory - heapMemory;
        recommendation = Math.max( mebiBytes( 100 ), recommendation );
        recommendation = Math.min( tebiBytes( 16 ), recommendation );
        return recommendation;
    }

    private static Brackets findMemoryBrackets( long totalMemoryBytes )
    {
        double totalMemoryGB = ((double) totalMemoryBytes) / ((double) gibiBytes( 1 ));
        Bracket lower = null;
        Bracket upper = null;
        for ( int i = 1; i < datapoints.length; i++ )
        {
            if ( totalMemoryGB < datapoints[i].totalMemory )
            {
                lower = datapoints[i - 1];
                upper = datapoints[i];
                break;
            }
        }
        if ( lower == null )
        {
            lower = datapoints[datapoints.length - 1];
            upper = datapoints[datapoints.length - 1];
        }
        return new Brackets( totalMemoryGB, lower, upper );
    }

    public static Arguments buildArgs()
    {
        String memory = bytesToString( OsBeanUtil.getTotalPhysicalMemory() );
        return new Arguments()
                .withArgument( new OptionalNamedArg( ARG_MEMORY, memory, memory,
                        "Recommend memory settings with respect to the given amount of memory, " +
                        "instead of the total memory of the system running the command." ) )
                .withDatabase(
                        "Name of specific database to calculate page cache memory requirement for. " +
                        "The generic calculation is still a good generic recommendation for this machine, " +
                        "but there will be an additional calculation for minimal required page cache memory " +
                        "for mapping all store and index files that are managed by the page cache." );
    }

    static String bytesToString( double bytes )
    {
        double gibi1 = ONE_GIBI_BYTE;
        double mebi1 = ONE_MEBI_BYTE;
        double mebi100 = 100 * mebi1;
        double kibi1 = ONE_KIBI_BYTE;
        double kibi100 = 100 * kibi1;
        if ( bytes >= gibi1 )
        {
            double gibibytes = bytes / gibi1;
            double modMebi = bytes % gibi1;
            if ( modMebi >= mebi100 )
            {
                return format( Locale.ROOT, "%dm", Math.round( bytes / mebi100 ) * 100 );
            }
            else
            {
                return format( Locale.ROOT, "%.0fg", gibibytes );
            }
        }
        else if ( bytes >= mebi1 )
        {
            double mebibytes = bytes / mebi1;
            double modKibi = bytes % mebi1;
            if ( modKibi >= kibi100 )
            {
                return format( Locale.ROOT, "%dk", Math.round( bytes / kibi100 ) * 100 );
            }
            else
            {
                return format( Locale.ROOT, "%.0fm", mebibytes );
            }
        }
        else
        {
            // For kilobytes there's no need to bother with decimals, just print a rough figure rounded upwards
            double kibiBytes = bytes / kibi1;
            return format( Locale.ROOT, "%dk", (long) Math.ceil( kibiBytes ) );
        }
    }

    MemoryRecommendationsCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.outsideWorld = outsideWorld;
        this.configDir = configDir;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        Arguments arguments = buildArgs().parse( args );

        String mem = arguments.get( ARG_MEMORY );
        long memory = buildSetting( ARG_MEMORY, BYTES ).build().apply( arguments::get );
        String os = bytesToString( recommendOsMemory( memory ) );
        String heap = bytesToString( recommendHeapMemory( memory ) );
        String pagecache = bytesToString( recommendPageCacheMemory( memory ) );
        boolean specificDb = arguments.has( ARG_DATABASE );

        print( "# Memory settings recommendation from neo4j-admin memrec:" );
        print( "#" );
        print( "# Assuming the system is dedicated to running Neo4j and has " + mem + " of memory," );
        print( "# we recommend a heap size of around " + heap + ", and a page cache of around " + pagecache + "," );
        print( "# and that about " + os + " is left for the operating system, and the native memory" );
        print( "# needed by Lucene and Netty." );
        print( "#" );
        print( "# Tip: If the indexing storage use is high, e.g. there are many indexes or most" );
        print( "# data indexed, then it might advantageous to leave more memory for the" );
        print( "# operating system." );
        print( "#" );
        print( "# Tip: The more concurrent transactions your workload has and the more updates" );
        print( "# they do, the more heap memory you will need. However, don't allocate more" );
        print( "# than 31g of heap, since this will disable pointer compression, also known as" );
        print( "# \"compressed oops\", in the JVM and make less effective use of the heap." );
        print( "#" );
        print( "# Tip: Setting the initial and the max heap size to the same value means the" );
        print( "# JVM will never need to change the heap size. Changing the heap size otherwise" );
        print( "# involves a full GC, which is desirable to avoid." );
        print( "#" );
        print( "# Based on the above, the following memory settings are recommended:" );
        print( initialHeapSize.name() + "=" + heap );
        print( maxHeapSize.name() + "=" + heap );
        print( pagecache_memory.name() + "=" + pagecache );

        if ( !specificDb )
        {
            return;
        }
        String databaseName = arguments.get( ARG_DATABASE );
        File configFile = configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile();
        File storeDir = getConfig( configFile, databaseName ).get( database_path );
        long pageCacheSize = dbSpecificPageCacheSize( storeDir );
        long luceneSize = dbSpecificLuceneSize( storeDir );

        print( "#" );
        print( "# The numbers below have been derived based on your current data volume in database and index configuration of database '" + databaseName +
                "'." );
        print( "# They can be used as an input into more detailed memory analysis." );
        print( "# Lucene indexes: " + bytesToString( luceneSize ) );
        print( "# Data volume and native indexes: " + bytesToString( pageCacheSize ) );
    }

    private long dbSpecificPageCacheSize( File storeDir )
    {
        return sumStoreFiles( storeDir ) + sumIndexFiles( baseSchemaIndexFolder( storeDir ), getNativeIndexFileFilter( storeDir, false ) );
    }

    private long dbSpecificLuceneSize( File storeDir )
    {
        return sumIndexFiles( baseSchemaIndexFolder( storeDir ), getNativeIndexFileFilter( storeDir, true ) );
    }

    private FilenameFilter getNativeIndexFileFilter( File storeDir, boolean inverse )
    {
        FileFilter nativeIndexFilter = new NativeIndexFileFilter( storeDir );
        return ( dir, name ) ->
        {
            File file = new File( dir, name );
            if ( outsideWorld.fileSystem().isDirectory( file ) )
            {
                // Always go down directories
                return true;
            }
            if ( name.equals( FailureStorage.DEFAULT_FAILURE_FILE_NAME ) )
            {
                // Never include failure-storage files
                return false;
            }

            return inverse != nativeIndexFilter.accept( file );
        };
    }

    private long sumStoreFiles( File storeDir )
    {
        long total = 0;
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                File file = new File( storeDir, type.getStoreFile().storeFileName() );
                if ( outsideWorld.fileSystem().fileExists( file ) )
                {
                    total += outsideWorld.fileSystem().getFileSize( file );
                }
            }
        }
        return total;
    }

    private long sumIndexFiles( File file, FilenameFilter filter )
    {
        long total = 0;
        if ( outsideWorld.fileSystem().isDirectory( file ) )
        {
            File[] children = outsideWorld.fileSystem().listFiles( file, filter );
            if ( children != null )
            {
                for ( File child : children )
                {
                    total += sumIndexFiles( child, filter );
                }
            }
        }
        else
        {
            total += outsideWorld.fileSystem().getFileSize( file );
        }
        return total;
    }

    private Config getConfig( File configFile, String databaseName ) throws CommandFailed
    {
        if ( !outsideWorld.fileSystem().fileExists( configFile ) )
        {
            throw new CommandFailed( "Unable to find config file, tried: " + configFile.getAbsolutePath() );
        }
        return Config.fromFile( configFile ).withHome( homeDir ).withSetting( active_database, databaseName ).withConnectorsDisabled().build();
    }

    private void print( String text )
    {
        outsideWorld.stdOutLine( text );
    }

    private static final class Bracket
    {
        private final double totalMemory;
        private final double osMemory;
        private final double heapMemory;

        private Bracket( double totalMemory, double osMemory, double heapMemory )
        {
            this.totalMemory = totalMemory;
            this.osMemory = osMemory;
            this.heapMemory = heapMemory;
        }

        double osMemory()
        {
            return osMemory;
        }

        double heapMemory()
        {
            return heapMemory;
        }
    }

    private static final class Brackets
    {
        private final double totalMemoryGB;
        private final Bracket lower;
        private final Bracket upper;

        private Brackets( double totalMemoryGB, Bracket lower, Bracket upper )
        {
            this.totalMemoryGB = totalMemoryGB;
            this.lower = lower;
            this.upper = upper;
        }

        private double differenceFactor()
        {
            if ( lower == upper )
            {
                return 0;
            }
            return (totalMemoryGB - lower.totalMemory) / (upper.totalMemory - lower.totalMemory);
        }

        public long recommend( ToDoubleFunction<Bracket> parameter )
        {
            double factor = differenceFactor();
            double lowerParam = parameter.applyAsDouble( lower );
            double upperParam = parameter.applyAsDouble( upper );
            double diff = upperParam - lowerParam;
            double recommend = lowerParam + (diff * factor);
            return mebiBytes( (long) (recommend * 1024.0) );
        }
    }
}
