/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Tool to dump content of {@link StoreType#NODE}, {@link StoreType#PROPERTY}, {@link StoreType#RELATIONSHIP} stores
 * into readable format.
 * @param <RECORD> record type to dump
 */
public abstract class DumpStoreChain<RECORD extends AbstractBaseRecord>
{
    private static final String REVERSE = "reverse";
    private static final String NODE = "node";
    private static final String FIRST = "first";
    private static final String RELS = "relationships";
    private static final String PROPS = "properties";
    private static final String RELSTORE = "neostore.relationshipstore.db";
    private static final String PROPSTORE = "neostore.propertystore.db";
    private static final String NODESTORE = "neostore.nodestore.db";

    public static void main( String... args ) throws Exception
    {
        Args arguments = Args.withFlags( REVERSE, RELS, PROPS ).parse( args );
        List<String> orphans = arguments.orphans();
        if ( orphans.size() != 1 )
        {
            throw invalidUsage( "no store file given" );
        }
        File storeFile = new File( orphans.get( 0 ) );
        DumpStoreChain tool;
        if ( storeFile.isDirectory() )
        {
            verifyFilesExists( new File( storeFile, NODESTORE ),
                    new File( storeFile, RELSTORE ),
                    new File( storeFile, PROPSTORE ) );
            tool = chainForNode( arguments );
        }
        else
        {
            verifyFilesExists( storeFile );
            if ( RELSTORE.equals( storeFile.getName() ) )
            {
                tool = relationshipChain( arguments );
            }
            else if ( PROPSTORE.equals( storeFile.getName() ) )
            {
                tool = propertyChain( arguments );
            }
            else
            {
                throw invalidUsage( "not a chain store: " + storeFile.getName() );
            }
        }
        tool.dump( storeFile );
    }

    long firstRecord;

    private DumpStoreChain( long firstRecord )
    {
        this.firstRecord = firstRecord;
    }

    private static LogProvider logProvider()
    {
        return Boolean.getBoolean( "logger" ) ? FormattedLogProvider.toOutputStream( System.out ) : NullLogProvider.getInstance();
    }

    void dump( File storeDir ) throws IOException
    {
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              PageCache pageCache = createPageCache( fs ) )
        {
            DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
            Config config = Config.defaults();
            StoreFactory storeFactory = new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fs,
                    logProvider(), EmptyVersionContextSupplier.EMPTY );

            try ( NeoStores neoStores = storeFactory.openNeoStores( getStoreTypes() ) )
            {
                RecordStore<RECORD> store = store( neoStores );
                RECORD record = store.newRecord();
                for ( long next = firstRecord; next != -1; )
                {
                    store.getRecord( next, record, RecordLoad.FORCE );
                    System.out.println( record );
                    next = next( record );
                }
            }
        }
    }

    private StoreType[] getStoreTypes()
    {
        return new StoreType[]{StoreType.NODE, StoreType.PROPERTY, StoreType.RELATIONSHIP};
    }

    abstract long next( RECORD record );

    abstract RecordStore<RECORD> store( NeoStores neoStores );

    private static DumpStoreChain propertyChain( Args args )
    {
        boolean reverse = verifyParametersAndCheckReverse( args, FIRST );
        return new DumpPropertyChain( Long.parseLong( args.get( FIRST, null ) ), reverse );
    }

    private static DumpStoreChain relationshipChain( Args args )
    {
        boolean reverse = verifyParametersAndCheckReverse( args, FIRST, NODE );
        long node = Long.parseLong( args.get( NODE, null ) );
        return new DumpRelationshipChain( Long.parseLong( args.get( FIRST, null ) ), node, reverse );
    }

    private static DumpStoreChain chainForNode( Args args )
    {
        Set<String> kwArgs = args.asMap().keySet();
        verifyParameters( kwArgs, kwArgs.contains( RELS ) ? RELS : PROPS, NODE );
        final long node = Long.parseLong( args.get( NODE, null ) );
        if ( args.getBoolean( RELS, false, true ) )
        {
            return new DumpRelationshipChain( -1, node, false )
            {
                @Override
                RelationshipStore store( NeoStores neoStores )
                {
                    NodeRecord nodeRecord = nodeRecord( neoStores, node );
                    firstRecord = nodeRecord.isDense() ? -1 : nodeRecord.getNextRel();
                    return super.store( neoStores );
                }
            };
        }
        else if ( args.getBoolean( PROPS, false, true ) )
        {
            return new DumpPropertyChain( -1, false )
            {
                @Override
                PropertyStore store( NeoStores neoStores )
                {
                    firstRecord = nodeRecord( neoStores, node ).getNextProp();
                    return super.store( neoStores );
                }
            };
        }
        else
        {
            throw invalidUsage( String.format( "Must be either -%s or -%s", RELS, PROPS ) );
        }
    }

    private static NodeRecord nodeRecord( NeoStores neoStores, long id )
    {
        NodeStore nodeStore = neoStores.getNodeStore();
        return nodeStore.getRecord( id, nodeStore.newRecord(), FORCE );
    }

    private static void verifyFilesExists( File... files )
    {
        for ( File file : files )
        {
            if ( !file.isFile() )
            {
                throw invalidUsage( file + " does not exist" );
            }
        }
    }

    private static boolean verifyParametersAndCheckReverse( Args args, String... parameters )
    {
        Set<String> kwArgs = args.asMap().keySet();
        if ( kwArgs.contains( REVERSE ) )
        {
            parameters = Arrays.copyOf( parameters, parameters.length + 1 );
            parameters[parameters.length - 1] = REVERSE;
        }
        verifyParameters( kwArgs, parameters );
        return args.getBoolean( REVERSE, false, true );
    }

    private static void verifyParameters( Set<String> args, String... parameters )
    {
        if ( args.size() != parameters.length )
        {
            throw invalidUsage( "accepted/required parameters: " + Arrays.toString( parameters ) );
        }
        for ( String parameter : parameters )
        {
            if ( !args.contains( parameter ) )
            {
                throw invalidUsage( "accepted/required parameters: " + Arrays.toString( parameters ) );
            }
        }
    }

    private static Error invalidUsage( String message )
    {
        System.err.println( "invalid usage: " + message );
        System.exit( 1 );
        return null;
    }

    private static class DumpPropertyChain extends DumpStoreChain<PropertyRecord>
    {
        private final boolean reverse;

        DumpPropertyChain( long first, boolean reverse )
        {
            super( first );
            this.reverse = reverse;
        }

        @Override
        PropertyStore store( NeoStores neoStores )
        {
            return neoStores.getPropertyStore();
        }

        @Override
        long next( PropertyRecord record )
        {
            return reverse ? record.getPrevProp() : record.getNextProp();
        }
    }

    private static class DumpRelationshipChain extends DumpStoreChain<RelationshipRecord>
    {
        private final long node;
        private final boolean reverse;

        DumpRelationshipChain( long first, long node, boolean reverse )
        {
            super( first );
            this.node = node;
            this.reverse = reverse;
        }

        @Override
        RelationshipStore store( NeoStores neoStores )
        {
            return neoStores.getRelationshipStore();
        }

        @Override
        long next( RelationshipRecord record )
        {
            if ( record.getFirstNode() == node )
            {
                return reverse ? record.getFirstPrevRel() : record.getFirstNextRel();
            }
            else if ( record.getSecondNode() == node )
            {
                return reverse ? record.getSecondPrevRel() : record.getSecondNextRel();
            }
            else
            {
                return -1;
            }
        }
    }
}
