/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.index.impl.lucene.EntityId.IdData;
import org.neo4j.index.lucene.QueryContext;

import static java.util.Collections.emptyList;

import static org.neo4j.index.impl.lucene.LuceneDataSource.LUCENE_VERSION;
import static org.neo4j.index.impl.lucene.LuceneIndex.KEY_DOC_ID;

class FullTxData extends TxData
{
    /*
     * The concept of orphan exists to find entities when querying where the transaction state
     * (i.e. a FullTxData object) has seen removed entities w/o key and potentially also w/o value.
     * A TxData instance receiving "add" calls with null key/value is an instance used to track removals.
     * A Lucene document storing state about e.g. {@code index.remove( myNode, "name" )}
     * <pre>
     * {
     *     __all__: "name"
     * }
     * </pre>
     *
     * A Lucene document storing state about e.g. {@code index.remove( myNode )}
     * <pre>
     * {
     *     __all__: "1"
     * }
     * where queries would (if there are any orphans at all stored) include the "all orphans" value ("1") as
     * well as any specific key which is pulled out from the incoming query.
     */
    private static final String ORPHANS_KEY = "__all__";
    /**
     * When querying we need to distinguish between documents coming from the store and documents
     * coming from transaction state. A field with this key is put on all documents in transaction state.
     */
    public static final String TX_STATE_KEY = "__tx_state__";
    private static final byte[] TX_STATE_VALUE = new byte[] {1};
    private static final String ORPHANS_VALUE = "1";

    private Directory directory;
    private IndexWriter writer;
    private boolean modified;
    private IndexReader reader;
    private IndexSearcher searcher;
    private final Map<Long, Document> cachedDocuments = new HashMap<Long, Document>();
    private Set<String> orphans;

    FullTxData( LuceneIndex index )
    {
        super( index );
    }

    @Override
    void add( TxDataHolder holder, EntityId entityId, String key, Object value )
    {
        try
        {
            ensureLuceneDataInstantiated();
            long id = entityId.id();
            Document document = findDocument( id );
            boolean add = false;
            if ( document == null )
            {
                document = IndexType.newDocument( entityId );
                document.add( new Field( TX_STATE_KEY, TX_STATE_VALUE ) );
                cachedDocuments.put( id, document );
                add = true;
            }

            if ( key == null && value == null )
            {
                // Set a special "always hit" flag
                document.add( new Field( ORPHANS_KEY, ORPHANS_VALUE, Store.NO, Index.NOT_ANALYZED ) );
                addOrphan( null );
            }
            else if ( value == null )
            {
                // Set a special "always hit" flag
                document.add( new Field( ORPHANS_KEY, key, Store.NO, Index.NOT_ANALYZED ) );
                addOrphan( key );
            }
            else
            {
                index.type.addToDocument( document, key, value );
            }

            if ( add )
            {
                writer.addDocument( document );
            }
            else
            {
                writer.updateDocument( index.type.idTerm( id ), document );
            }
            invalidateSearcher();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void addOrphan( String key )
    {
        if ( orphans == null )
        {
            orphans = new HashSet<>();
        }
        orphans.add( key );
    }

    private Document findDocument( long id )
    {
        return cachedDocuments.get( id );
    }

    private void ensureLuceneDataInstantiated()
    {
        if ( this.directory == null )
        {
            try
            {
                this.directory = new RAMDirectory();
                IndexWriterConfig writerConfig = new IndexWriterConfig( LUCENE_VERSION, index.type.analyzer );
                this.writer = new IndexWriter( directory, writerConfig );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    @Override
    void remove( TxDataHolder holder, EntityId entityId, String key, Object value )
    {
        try
        {
            ensureLuceneDataInstantiated();
            long id = entityId.id();
            Document document = findDocument( id );
            if ( document != null )
            {
                index.type.removeFromDocument( document, key, value );
                if ( LuceneDataSource.documentIsEmpty( document ) )
                {
                    writer.deleteDocuments( index.type.idTerm( id ) );
                }
                else
                {
                    writer.updateDocument( index.type.idTerm( id ), document );
                }
            }
            invalidateSearcher();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    Collection<EntityId> query( TxDataHolder holder, Query query, QueryContext contextOrNull )
    {
        return internalQuery( query, contextOrNull );
    }

    private Collection<EntityId> internalQuery( Query query, QueryContext contextOrNull )
    {
        if ( this.directory == null )
        {
            return Collections.emptySet();
        }

        try
        {
            Sort sorting = contextOrNull != null ? contextOrNull.getSorting() : null;
            boolean prioritizeCorrectness = contextOrNull == null || !contextOrNull.getTradeCorrectnessForSpeed();
            IndexSearcher theSearcher = searcher( prioritizeCorrectness );
            query = includeOrphans( query );
            Hits hits = new Hits( theSearcher, query, null, sorting, prioritizeCorrectness );
            Collection<EntityId> result = new ArrayList<>();
            for ( int i = 0; i < hits.length(); i++ )
            {
                result.add( new IdData( Long.valueOf( hits.doc( i ).get( KEY_DOC_ID ) ) ) );
            }
            return result;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Query includeOrphans( Query query )
    {
        if ( orphans == null )
        {
            return query;
        }

        BooleanQuery result = new BooleanQuery();
        result.add( injectOrphans( query ), Occur.SHOULD );
        result.add( new TermQuery( new Term( ORPHANS_KEY, ORPHANS_VALUE ) ), Occur.SHOULD );
        return result;
    }

    private Query injectOrphans( Query query )
    {
        if ( query instanceof BooleanQuery )
        {
            BooleanQuery source = (BooleanQuery) query;
            BooleanQuery result = new BooleanQuery();
            for ( BooleanClause clause : source.clauses() )
            {
                result.add( injectOrphans( clause.getQuery() ), clause.getOccur() );
            }
            return result;
        }

        String orphanField = extractTermField( query );
        if ( orphanField == null )
        {
            return query;
        }

        BooleanQuery result = new BooleanQuery();
        result.add( query, Occur.SHOULD );
        result.add( new TermQuery( new Term( ORPHANS_KEY, orphanField ) ), Occur.SHOULD );
        return result;
    }

    private String extractTermField( Query query )
    {
        // Try common types of queries
        if ( query instanceof TermQuery )
        {
            return ((TermQuery)query).getTerm().field();
        }
        else if ( query instanceof WildcardQuery )
        {
            return ((WildcardQuery)query).getTerm().field();
        }
        else if ( query instanceof PrefixQuery )
        {
            return ((PrefixQuery)query).getPrefix().field();
        }
        else if ( query instanceof MatchAllDocsQuery )
        {
            return null;
        }

        // Try to extract terms and get it that way
        String field = getFieldFromExtractTerms( query );
        if ( field != null )
        {
            return field;
        }

        // Last resort: since Query doesn't have a common interface for getting
        // the term/field of its query this is one option.
        return getFieldViaReflection( query );
    }

    private String getFieldViaReflection( Query query )
    {
        try
        {
            try
            {
                Term term = (Term) query.getClass().getMethod( "getTerm" ).invoke( query );
                return term.field();
            }
            catch ( NoSuchMethodException e )
            {
                return (String) query.getClass().getMethod( "getField" ).invoke( query );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private String getFieldFromExtractTerms( Query query )
    {
        Set<Term> terms = new HashSet<Term>();
        try
        {
            query.extractTerms( terms );
        }
        catch ( UnsupportedOperationException e )
        {
            // In case of wildcard/range queries try to rewrite the query
            // i.e. get the terms from the reader.
            try
            {
                query.rewrite( reader ).extractTerms( terms );
            }
            catch ( IOException ioe )
            {
                throw new UnsupportedOperationException( ioe );
            }
            catch ( UnsupportedOperationException ue )
            {
                // TODO This is for "*" queries and such. Lucene doesn't seem
                // to be able/willing to rewrite such queries.
                // Just ignore the orphans then... OK?
            }
        }
        return terms.isEmpty() ? null : terms.iterator().next().field();
    }

    @Override
    void close()
    {
        LuceneUtil.close( this.writer );
        LuceneUtil.close( this.reader );
        LuceneUtil.close( this.searcher );
    }

    private void invalidateSearcher()
    {
        this.modified = true;
    }

    private IndexSearcher searcher( boolean allowRefreshSearcher )
    {
        if ( this.searcher != null && (!modified || !allowRefreshSearcher) )
        {
            return this.searcher;
        }

        try
        {
            IndexReader newReader = this.reader == null ? IndexReader.open( this.writer, true ) : this.reader.reopen();
            if ( newReader == this.reader )
            {
                return this.searcher;
            }
            LuceneUtil.close( reader );
            this.reader = newReader;
            LuceneUtil.close( searcher );
            searcher = new IndexSearcher( reader );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if ( allowRefreshSearcher )
            {
                this.modified = false;
            }
        }
        return this.searcher;
    }

    @Override
    IndexSearcher asSearcher( TxDataHolder holder, QueryContext context )
    {
        boolean refresh = context == null || !context.getTradeCorrectnessForSpeed();
        return searcher( refresh );
    }

    @Override
    Collection<EntityId> get( TxDataHolder holder, String key, Object value )
    {
        return internalQuery( index.type.get( key, value ), null );
    }

    @Override
    Collection<EntityId> getOrphans( String key )
    {
        return emptyList();
    }
}
