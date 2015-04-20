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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;

public abstract class DumpStoreChain<RECORD extends AbstractBaseRecord>
{
    private static final String REVERSE = "reverse", NODE = "node", FIRST = "first",
            RELS = "relationships", PROPS = "properties";
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

    long first;

    private DumpStoreChain( long first )
    {
        this.first = first;
    }

    private static StringLogger logger()
    {
        return Boolean.getBoolean( "logger" ) ? StringLogger.SYSTEM : StringLogger.DEV_NULL;
    }

    void dump( File storeFile ) throws IOException
    {
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

        try ( PageCache pageCache = createPageCache( fs ) )
        {
            DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
            Config config = new Config();
            StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory, pageCache, fs, logger(), null );
            RecordStore<RECORD> store = store( storeFactory, storeFile );
            try
            {
                for ( long next = first; next != -1; )
                {
                    RECORD record = store.forceGetRecord( next );
                    System.out.println( record );
                    next = next( record );
                }
            }
            finally
            {
                store.close();
            }
        }
    }

    abstract long next( RECORD record );

    abstract RecordStore<RECORD> store( StoreFactory factory, File storeFile );

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
                RelationshipStore store( StoreFactory factory, File storeFile )
                {
                    NodeRecord nodeRecord = nodeRecord( factory, storeFile, node );
                    first = nodeRecord.isDense() ? -1 : nodeRecord.getNextRel();
                    return super.store( factory, new File( storeFile, RELSTORE ) );
                }
            };
        }
        else if ( args.getBoolean( PROPS, false, true ) )
        {
            return new DumpPropertyChain( -1, false )
            {
                @Override
                PropertyStore store( StoreFactory factory, File storeFile )
                {
                    first = nodeRecord( factory, storeFile, node ).getNextProp();
                    return super.store( factory, new File( storeFile, PROPSTORE ) );
                }
            };
        }
        else
        {
            throw invalidUsage( String.format( "Must be either -%s or -%s", RELS, PROPS ) );
        }
    }

    private static NodeRecord nodeRecord( StoreFactory factory, File storeDir, long id )
    {
        try ( NodeStore store = factory.newNodeStore( new File( storeDir, NODESTORE ) ) )
        {
            return store.forceGetRecord( id );
        }
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
        PropertyStore store( StoreFactory factory, File storeFile )
        {
            return factory.newPropertyStore( storeFile );
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
        RelationshipStore store( StoreFactory factory, File storeFile )
        {
            return factory.newRelationshipStore( storeFile );
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
