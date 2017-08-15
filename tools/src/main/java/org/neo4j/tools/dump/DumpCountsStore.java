/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.kvstore.HeaderField;
import org.neo4j.kernel.impl.store.kvstore.Headers;
import org.neo4j.kernel.impl.store.kvstore.MetadataVisitor;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.Token;

import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;

/**
 * Tool that will dump content of count store content into a simple string representation for further analysis.
 */
public class DumpCountsStore implements CountsVisitor, MetadataVisitor, UnknownKey.Visitor
{
    public static void main( String... args ) throws IOException
    {
        if ( args.length != 1 )
        {
            System.err.println( "Expecting exactly one argument describing the path to the store" );
            System.exit( 1 );
        }
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            dumpCountsStore( fileSystem, new File( args[0] ), System.out );
        }
    }

    public static void dumpCountsStore( FileSystemAbstraction fs, File path, PrintStream out ) throws IOException
    {
        try ( PageCache pages = createPageCache( fs );
              Lifespan life = new Lifespan() )
        {
            if ( fs.isDirectory( path ) )
            {
                StoreFactory factory = new StoreFactory( path, pages, fs, NullLogProvider.getInstance() );

                NeoStores neoStores = factory.openAllNeoStores();
                SchemaStorage schemaStorage = new SchemaStorage( neoStores.getSchemaStore() );
                neoStores.getCounts().accept( new DumpCountsStore( out, neoStores, schemaStorage ) );
            }
            else
            {
                VisitableCountsTracker tracker = new VisitableCountsTracker(
                        NullLogProvider.getInstance(), fs, pages, Config.defaults(), path );
                if ( fs.fileExists( path ) )
                {
                    tracker.visitFile( path, new DumpCountsStore( out ) );
                }
                else
                {
                    life.add( tracker ).accept( new DumpCountsStore( out ) );
                }
            }
        }
    }

    DumpCountsStore( PrintStream out )
    {
        this( out, Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList() );
    }

    DumpCountsStore( PrintStream out, NeoStores neoStores, SchemaStorage schemaStorage )
    {
        this( out, getAllIndexesFrom( schemaStorage ),
              allTokensFrom( neoStores.getLabelTokenStore() ),
              allTokensFrom( neoStores.getRelationshipTypeTokenStore() ),
              allTokensFrom( neoStores.getPropertyKeyTokenStore() ) );
    }

    private final PrintStream out;
    private final Map<Long,IndexDescriptor> indexes;
    private final List<Token> labels;
    private final List<RelationshipTypeToken> relationshipTypes;
    private final List<Token> propertyKeys;

    private DumpCountsStore( PrintStream out, Map<Long,IndexDescriptor> indexes, List<Token> labels,
            List<RelationshipTypeToken> relationshipTypes,
            List<Token> propertyKeys )
    {
        this.out = out;
        this.indexes = indexes;
        this.labels = labels;
        this.relationshipTypes = relationshipTypes;
        this.propertyKeys = propertyKeys;
    }

    @Override
    public void visitMetadata( File file, Headers headers, int entryCount )
    {
        out.printf( "Counts Store:\t%s%n", file );
        for ( HeaderField<?> headerField : headers.fields() )
        {
            out.printf( "%s:\t%s%n", headerField.toString(), headers.get( headerField ) );
        }
        out.printf( "\tentries:\t%d%n", entryCount );
        out.println( "Entries:" );
    }

    @Override
    public void visitNodeCount( int labelId, long count )
    {
        out.printf( "\tNode[(%s)]:\t%d%n", label( labelId ), count );
    }

    @Override
    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        out.printf( "\tRelationship[(%s)-%s->(%s)]:\t%d%n",
                    label( startLabelId ), relationshipType( typeId ), label( endLabelId ),
                    count );
    }

    @Override
    public void visitIndexStatistics( long indexId, long updates, long size )
    {
        IndexDescriptor index = indexes.get( indexId );
        out.printf( "\tIndexStatistics[(%s {%s})]:\tupdates=%d, size=%d%n",
                label( index.schema().getLabelId() ), propertyKeys( index.schema().getPropertyIds() ), updates, size );
    }

    @Override
    public void visitIndexSample( long indexId, long unique, long size )
    {
        IndexDescriptor index = indexes.get( indexId );
        out.printf( "\tIndexSample[(%s {%s})]:\tunique=%d, size=%d%n",
                label( index.schema().getLabelId() ), propertyKeys( index.schema().getPropertyIds() ), unique, size );
    }

    @Override
    public boolean visitUnknownKey( ReadableBuffer key, ReadableBuffer value )
    {
        out.printf( "\t%s:\t%s%n", key, value );
        return true;
    }

    private String label( int id )
    {
        if ( id == ReadOperations.ANY_LABEL )
        {
            return "";
        }
        return token( new StringBuilder(), labels, ":", "label", id ).toString();
    }

    private String propertyKeys( int[] ids )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( "," );
            }
            token( builder, propertyKeys, "", "key", ids[i] );
        }
        return builder.toString();
    }

    private String relationshipType( int id )
    {
        if ( id == ReadOperations.ANY_RELATIONSHIP_TYPE )
        {
            return "";
        }
        return token( new StringBuilder().append( '[' ), relationshipTypes, ":", "type", id ).append( ']' ).toString();
    }

    private static StringBuilder token( StringBuilder result, List<? extends Token> tokens, String pre, String handle, int id )
    {
        Token token = null;
        // search backwards for the token
        for ( int i = (id < tokens.size()) ? id : tokens.size() - 1; i >= 0; i-- )
        {
            token = tokens.get(i);
            if ( token.id() == id )
            {
                break; // found
            }
            if ( token.id() < id )
            {
                token = null; // not found
                break;
            }
        }
        if ( token != null )
        {
            String name = token.name();
            result.append( pre ).append( name )
                  .append( " [" ).append( handle ).append( "Id=" ).append( token.id() ).append( ']' );
        }
        else
        {
            result.append( handle ).append( "Id=" ).append( id );
        }
        return result;
    }

    private static <TOKEN extends Token> List<TOKEN> allTokensFrom( TokenStore<?, TOKEN> store )
    {
        try ( TokenStore<?, TOKEN> tokens = store )
        {
            return tokens.getTokens( Integer.MAX_VALUE );
        }
    }

    private static Map<Long,IndexDescriptor> getAllIndexesFrom( SchemaStorage storage )
    {
        HashMap<Long,IndexDescriptor> indexes = new HashMap<>();
        Iterator<IndexRule> indexRules = storage.indexesGetAll();
        while ( indexRules.hasNext() )
        {
            IndexRule rule = indexRules.next();
            indexes.put( rule.getId(), rule.getIndexDescriptor() );
        }
        return indexes;
    }

    private static class VisitableCountsTracker extends CountsTracker
    {

        VisitableCountsTracker( LogProvider logProvider, FileSystemAbstraction fs,
                PageCache pages, Config config, File baseFile )
        {
            super( logProvider, fs, pages, config, baseFile );
        }

        @Override
        public void visitFile( File path, CountsVisitor visitor ) throws IOException
        {
            super.visitFile( path, visitor );
        }
    }
}
