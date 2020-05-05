/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.commandline.dbms.storeutil;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.NonUniqueTokenException;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.token.api.TokensLoader;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.internal.recordstorage.StoreTokens.allReadableTokens;
import static org.neo4j.internal.recordstorage.StoreTokens.createReadOnlyTokenHolder;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.logging.Level.DEBUG;
import static org.neo4j.logging.Level.INFO;
import static org.neo4j.procedure.builtin.SchemaStatementProcedure.createStatement;

public class StoreCopy
{
    private final DatabaseLayout from;
    private final Config config;
    private final boolean verbose;
    private final FormatEnum format;
    private final List<String> deleteNodesWithLabels;
    private final List<String> skipLabels;
    private final List<String> skipProperties;
    private final List<String> skipRelationships;
    private final PrintStream out;

    private StoreCopyFilter storeCopyFilter;
    private MutableMap<String, List<NamedToken>> recreatedTokens;
    private TokenHolders tokenHolders;
    private NodeStore nodeStore;
    private PropertyStore propertyStore;
    private RelationshipStore relationshipStore;
    private StoreCopyStats stats;

    public enum FormatEnum
    {
        same,
        standard,
        high_limit
    }

    public StoreCopy( DatabaseLayout from, Config config, FormatEnum format, List<String> deleteNodesWithLabels, List<String> skipLabels,
            List<String> skipProperties, List<String> skipRelationships, boolean verbose, PrintStream out )
    {
        this.from = from;
        this.config = config;
        this.format = format;
        this.deleteNodesWithLabels = deleteNodesWithLabels;
        this.skipLabels = skipLabels;
        this.skipProperties = skipProperties;
        this.skipRelationships = skipRelationships;
        this.out = out;
        this.verbose = verbose;
    }

    public void copyTo( DatabaseLayout toDatabaseLayout, String fromPageCacheMemory, String toPageCacheMemory ) throws Exception
    {
        Path logFilePath = getLogFilePath( config );
        try ( OutputStream logFile = new BufferedOutputStream( Files.newOutputStream( logFilePath ) ) )
        {
            LogProvider logProvider = new DuplicatingLogProvider( getLog( logFile ), getLog( out ) );
            Log log = logProvider.getLog( "StoreCopy" );
            try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                    JobScheduler scheduler = createInitialisedScheduler();
                    PageCache fromPageCache = createPageCache( fs, fromPageCacheMemory, scheduler );
                    PageCache toPageCache = createPageCache( fs, toPageCacheMemory, scheduler );
                    NeoStores neoStores =
                            new StoreFactory( from, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), fromPageCache, fs, NullLogProvider.getInstance() )
                                    .openAllNeoStores() )
            {
                out.println( "Starting to copy store, output will be saved to: " + logFilePath.toAbsolutePath() );
                nodeStore = neoStores.getNodeStore();
                propertyStore = neoStores.getPropertyStore();
                relationshipStore = neoStores.getRelationshipStore();
                recreatedTokens = Maps.mutable.empty();
                tokenHolders = createTokenHolders( neoStores );
                stats = new StoreCopyStats( log );
                SchemaStore schemaStore = neoStores.getSchemaStore();

                storeCopyFilter = convertFilter( deleteNodesWithLabels, skipLabels, skipProperties, skipRelationships, tokenHolders, stats );

                RecordFormats recordFormats = setupRecordFormats( neoStores, format );

                ExecutionMonitor executionMonitor = verbose ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, out,
                        SpectrumExecutionMonitor.DEFAULT_WIDTH ) : ExecutionMonitors.defaultVisible();

                log.info( "### Copy Data ###" );
                log.info( "Source: %s (page cache %s)", from.databaseDirectory(), fromPageCacheMemory );
                log.info( "Target: %s (page cache %s)", toDatabaseLayout.databaseDirectory(), toPageCacheMemory );
                log.info( "Empty database created, will start importing readable data from the source." );

                BatchImporter batchImporter = BatchImporterFactory.withHighestPriority().instantiate(
                        toDatabaseLayout, fs, toPageCache, Configuration.DEFAULT, new SimpleLogService( logProvider ), executionMonitor,
                        AdditionalInitialIds.EMPTY, config, recordFormats, NO_MONITOR, null, Collector.EMPTY,
                        TransactionLogInitializer.getLogFilesInitializer() );

                batchImporter.doImport( Input.input( this::nodeIterator, this::relationshipIterator, IdType.INTEGER, getEstimates(), new Groups() ) );

                stats.printSummary();

                // Display schema information
                log.info( "### Extracting schema ###" );
                log.info( "Trying to extract schema..." );
                Map<String,String> schemaStatements = getSchemaStatements( stats, schemaStore, tokenHolders );
                int schemaCount = schemaStatements.size();
                if ( schemaCount == 0 )
                {
                    log.info( "... found %d schema definitions.", schemaCount );
                }
                else
                {
                    log.info( "... found %d schema definitions. The following can be used to recreate the schema:", schemaCount );
                    String newLine = System.lineSeparator();
                    log.info( newLine + newLine + String.join( ";" + newLine, schemaStatements.values() ) );
                    log.info( "You have to manually apply the above commands to the database when it is stared to recreate the indexes and constraints. " +
                            "The commands are saved to " + logFilePath.toAbsolutePath() + " as well for reference.");
                }

                if ( recreatedTokens.notEmpty() )
                {
                    log.info( "The following tokens were recreated (with new names) in order to not leave data behind:" );
                    recreatedTokens.forEach( ( type, tokens ) ->
                    {
                        for ( NamedToken token : tokens )
                        {
                            log.info( "   `%s` (with id %s(%s)).", token.name(), type, token.id() );
                        }
                    } );
                }
            }
        }
    }

    private TokenHolders createTokenHolders( NeoStores neoStores )
    {
        TokenHolder propertyKeyTokens = createTokenHolder( TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens = createTokenHolder( TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens = createTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        tokenHolders.setInitialTokens( filterDuplicateTokens( allReadableTokens( neoStores ) ) );
        return tokenHolders;
    }

    private TokenHolder createTokenHolder( String tokenType )
    {
        return new TokenHolder()
        {
            private final TokenHolder delegate = createReadOnlyTokenHolder( tokenType );
            private int createdTokenCounter; // Guarded by 'this'.

            @Override
            public synchronized void setInitialTokens( List<NamedToken> tokens ) throws NonUniqueTokenException
            {
                delegate.setInitialTokens( tokens );
            }

            @Override
            public void addToken( NamedToken token ) throws NonUniqueTokenException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getOrCreateId( String name )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void getOrCreateIds( String[] names, int[] ids )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public synchronized NamedToken getTokenById( int id )
            {
                try
                {
                    return delegate.getTokenById( id );
                }
                catch ( TokenNotFoundException e )
                {
                    stats.addCorruptToken( tokenType, id );
                    String tokenName;
                    do
                    {
                        createdTokenCounter++;
                        tokenName = getTokenType() + "_" + createdTokenCounter;
                    }
                    while ( getIdByName( tokenName ) != TokenConstants.NO_TOKEN );
                    NamedToken token = new NamedToken( tokenName, id );
                    delegate.addToken( token );
                    recreatedTokens.getIfAbsentPut( getTokenType(), ArrayList::new ).add( token );
                    return token;
                }
            }

            @Override
            public synchronized int getIdByName( String name )
            {
                return delegate.getIdByName( name );
            }

            @Override
            public synchronized boolean getIdsByNames( String[] names, int[] ids )
            {
                return delegate.getIdsByNames( names, ids );
            }

            @Override
            public synchronized Iterable<NamedToken> getAllTokens()
            {
                return delegate.getAllTokens();
            }

            @Override
            public synchronized String getTokenType()
            {
                return delegate.getTokenType();
            }

            @Override
            public synchronized boolean hasToken( int id )
            {
                return delegate.hasToken( id );
            }

            @Override
            public synchronized int size()
            {
                return delegate.size();
            }

            @Override
            public void getOrCreateInternalIds( String[] names, int[] ids )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public synchronized NamedToken getInternalTokenById( int id ) throws TokenNotFoundException
            {
                return delegate.getInternalTokenById( id );
            }
        };
    }

    private TokensLoader filterDuplicateTokens( TokensLoader loader )
    {
        return new TokensLoader()
        {
            @Override
            public List<NamedToken> getPropertyKeyTokens()
            {
                return unique( loader.getPropertyKeyTokens() );
            }

            @Override
            public List<NamedToken> getLabelTokens()
            {
                return unique( loader.getLabelTokens() );
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens()
            {
                return unique( loader.getRelationshipTypeTokens() );
            }

            private List<NamedToken> unique( List<NamedToken> tokens )
            {
                if ( !tokens.isEmpty() )
                {
                    Set<String> names = new HashSet<>( tokens.size() );
                    int i = 0;
                    while ( i < tokens.size() )
                    {
                        if ( names.add( tokens.get( i ).name() ) )
                        {
                            i++;
                        }
                        else
                        {
                            removeUnordered( tokens, i );
                        }
                    }
                }
                return tokens;
            }

            /**
             * Remove the token at the given index, by replacing it with the last token in the list.
             * This changes the order of elements, but can be done in constant time instead of linear time.
             */
            private void removeUnordered( List<NamedToken> list, int index )
            {
                int lastIndex = list.size() - 1;
                NamedToken endToken = list.remove( lastIndex );
                if ( index < lastIndex )
                {
                    list.set( index, endToken );
                }
            }
        };
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, String memory, JobScheduler jobScheduler )
    {
        SingleFilePageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.open( fileSystem );
        MemoryAllocator memoryAllocator = createAllocator( memory, EmptyMemoryTracker.INSTANCE );
        return new MuninnPageCache( swapperFactory, memoryAllocator, PageCacheTracer.NULL, DefaultPageCursorTracerSupplier.INSTANCE,
                EmptyVersionContextSupplier.EMPTY, jobScheduler );
    }

    private LogProvider getLog( OutputStream out )
    {
        return FormattedLogProvider
                .withZoneId( config.get( GraphDatabaseSettings.db_timezone ).getZoneId() )
                .withDefaultLogLevel( verbose ? DEBUG : INFO )
                .toOutputStream( out );
    }

    private static Path getLogFilePath( Config config )
    {
        return config.get( logs_directory ).resolve( format( "neo4j-admin-copy-%s.log", new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( new Date() ) ) );
    }

    private static Map<String,String> getSchemaStatements( StoreCopyStats stats, SchemaStore schemaStore,
            TokenHolders tokenHolders )
    {
        TokenRead tokenRead = new ReadOnlyTokenRead( tokenHolders );
        SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( schemaStore, tokenHolders );
        Map<String,IndexDescriptor> indexes = new HashMap<>();
        List<ConstraintDescriptor> constraints = new ArrayList<>();
        schemaRuleAccess.indexesGetAllIgnoreMalformed().forEachRemaining( i -> indexes.put( i.getName(), i ) );
        schemaRuleAccess.constraintsGetAllIgnoreMalformed().forEachRemaining(constraints::add );

        Map<String,String> schemaStatements = new HashMap<>();
        for ( var entry : indexes.entrySet() )
        {
            String statement;
            try
            {
                statement = createStatement( tokenRead, entry.getValue() );
            }
            catch ( Exception e )
            {
                stats.invalidIndex( entry.getValue(), e );
                continue;
            }
            schemaStatements.put( entry.getKey(), statement );
        }
        for ( ConstraintDescriptor constraint : constraints )
        {
            String statement;
            try
            {
                statement = createStatement( indexes::get, tokenRead, constraint );
            }
            catch ( Exception e )
            {
                stats.invalidConstraint( constraint, e );
                continue;
            }
            schemaStatements.put( constraint.getName(), statement );
        }
        return schemaStatements;
    }

    private static RecordFormats setupRecordFormats( NeoStores neoStores, FormatEnum format )
    {
        if ( format == FormatEnum.same )
        {
            return neoStores.getRecordFormats();
        }
        else if ( format == FormatEnum.high_limit )
        {
            try
            {
                return RecordFormatSelector.selectForConfig( Config.defaults( GraphDatabaseSettings.record_format, "high_limit" ),
                        NullLogProvider.getInstance() );
            }
            catch ( IllegalArgumentException e )
            {
                throw new IllegalArgumentException( "Unable to load high-limit format, make sure you are using the correct version of neo4j.", e );
            }
        }
        else
        {
            return Standard.LATEST_RECORD_FORMATS;
        }
    }

    private static StoreCopyFilter convertFilter( List<String> deleteNodesWithLabels, List<String> skipLabels, List<String> skipProperties,
            List<String> skipRelationships, TokenHolders tokenHolders, StoreCopyStats stats )
    {
        int[] deleteNodesWithLabelsIds = getTokenIds( deleteNodesWithLabels, tokenHolders.labelTokens() );
        int[] skipLabelsIds = getTokenIds( skipLabels, tokenHolders.labelTokens() );
        int[] skipPropertyIds = getTokenIds( skipProperties, tokenHolders.propertyKeyTokens() );
        int[] skipRelationshipIds = getTokenIds( skipRelationships, tokenHolders.relationshipTypeTokens() );

        return new StoreCopyFilter( stats, deleteNodesWithLabelsIds, skipLabelsIds, skipPropertyIds, skipRelationshipIds );
    }

    private static int[] getTokenIds( List<String> tokenNames, TokenHolder tokenHolder )
    {
        int[] labelIds = new int[tokenNames.size()];
        int i = 0;
        for ( String tokenName : tokenNames )
        {
            int labelId = tokenHolder.getIdByName( tokenName );
            if ( labelId == TokenConstants.NO_TOKEN )
            {
                throw new RuntimeException( "Unable to find token: " + tokenName );
            }
            labelIds[i++] = labelId;
        }
        return labelIds;
    }

    private Input.Estimates getEstimates()
    {
        long propertyStoreSize = storeSize( propertyStore ) / 2 +
                storeSize( propertyStore.getStringStore() ) / 2 +
                storeSize( propertyStore.getArrayStore() ) / 2;
        return Input.knownEstimates(
                nodeStore.getNumberOfIdsInUse(),
                relationshipStore.getNumberOfIdsInUse(),
                propertyStore.getNumberOfIdsInUse(),
                propertyStore.getNumberOfIdsInUse(),
                propertyStoreSize / 2,
                propertyStoreSize / 2,
                tokenHolders.labelTokens().size() );
    }

    private static long storeSize( CommonAbstractStore<? extends AbstractBaseRecord,? extends StoreHeader> store )
    {
        try
        {
            return store.getStoreSize();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private LenientInputChunkIterator nodeIterator()
    {
        return new LenientInputChunkIterator( nodeStore )
        {
            @Override
            public InputChunk newChunk()
            {
                return new LenientNodeReader( stats, nodeStore, propertyStore, tokenHolders, storeCopyFilter );
            }
        };
    }

    private LenientInputChunkIterator relationshipIterator()
    {
        return new LenientInputChunkIterator( relationshipStore )
        {
            @Override
            public InputChunk newChunk()
            {
                return new LenientRelationshipReader( stats, relationshipStore, propertyStore, tokenHolders, storeCopyFilter );
            }
        };
    }
}
