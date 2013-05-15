/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.IndexType.instantiateField;
import static org.neo4j.index.impl.lucene.IndexType.newBaseDocument;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.neo4j.index.impl.lucene.LuceneUtil;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;

public class LuceneSchemaIndexProvider extends SchemaIndexProvider
{
    private final DirectoryFactory directoryFactory;
    private final DocumentLogic documentLogic = new DocumentLogic();
    private final WriterLogic writerLogic = new WriterLogic();
    private final File rootDirectory;
    
    public LuceneSchemaIndexProvider( DirectoryFactory directoryFactory, Config config )
    {
        super( LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR, 1 );
        this.directoryFactory = directoryFactory;
        this.rootDirectory = getRootDirectory( config, LuceneSchemaIndexProviderFactory.KEY );
    }
    
    private File dirFile( long indexId )
    {
        return new File( rootDirectory, "" + indexId );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
    {
        return new LuceneIndexPopulator( standard(), directoryFactory, dirFile( indexId ), 10000,
                documentLogic, writerLogic );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config )
    {
        // TODO: return a uniqueness enforcing IndexAccessor if config says so
        try
        {
            return new LuceneIndexAccessor( standard(), directoryFactory, dirFile( indexId ),
                    documentLogic, writerLogic );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        try
        {
            Directory directory = directoryFactory.open( dirFile( indexId ) );
            try {
                boolean status = writerLogic.hasOnlineStatus( directory );
                return status ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
            }
            finally {
                directory.close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    static class DocumentLogic
    {
        private static final String NODE_ID_KEY = "_id_";
        private static final String SINGLE_PROPERTY_KEY = "key";
        
        Document newDocument( long nodeId, Object value )
        {
            Document document = newBaseDocument( nodeId );
            document.add( new Field( NODE_ID_KEY, "" + nodeId, Store.YES, NOT_ANALYZED ) );
            document.add( instantiateField( SINGLE_PROPERTY_KEY, value, NOT_ANALYZED ) );
            return document;
        }

        public Query newQuery( Object value )
        {
            if ( value instanceof String )
            {
                return new TermQuery( new Term( SINGLE_PROPERTY_KEY, (String) value ) );
            }
            else if ( value instanceof Number )
            {
                Number number = (Number) value;
                return LuceneUtil.rangeQuery( SINGLE_PROPERTY_KEY, number, number, true, true );
            }
            throw new UnsupportedOperationException( value.toString() + ", " + value.getClass() );
        }
        
        public Term newQueryForChangeOrRemove( long nodeId )
        {
            return new Term( NODE_ID_KEY, "" + nodeId );
        }

        public long getNodeId( Document from )
        {
            return Long.parseLong( from.get( NODE_ID_KEY ) );
        }
    }
    
    static class WriterLogic
    {
        private static final String KEY_STATUS = "status";
        private static final String ONLINE = "online";
        
        public void forceAndMarkAsOnline( IndexWriter writer ) throws IOException
        {
            writer.commit( stringMap( KEY_STATUS, ONLINE ) );
        }
        
        public boolean hasOnlineStatus( Directory directory ) throws IOException
        {
            if ( !IndexReader.indexExists( directory ) )
                return false;

            IndexReader reader = null;
            try
            {
                reader = IndexReader.open( directory );
                Map<String, String> userData = reader.getIndexCommit().getUserData();
                return ONLINE.equals( userData.get( KEY_STATUS ) );
            }
            finally
            {
                if ( reader != null )
                {
                    reader.close();
                }
            }
        }

        public void close( IndexWriter writer ) throws IOException
        {
            writer.close( true );
        }
    }
}
