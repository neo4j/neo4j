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
package org.neo4j.kernel.api.impl.index.collector;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.impl.lucene.explicit.EmptyIndexHits;
import org.neo4j.kernel.impl.api.explicitindex.AbstractIndexHits;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

/**
 * Collector to record per-segment {@code DocIdSet}s and {@code LeafReaderContext}s for every
 * segment that contains a hit. Those items can be later used to read {@code DocValues} fields
 * and iterate over the matched {@code DocIdSet}s. This collector is different from
 * {@code org.apache.lucene.search.CachingCollector} in that the later focuses on predictable RAM usage
 * and feeding other collectors while this collector focuses on exposing the required per-segment data structures
 * to the user.
 */
public class DocValuesCollector extends SimpleCollector
{
    private static final EmptyIndexHits<Document> EMPTY_INDEX_HITS = new EmptyIndexHits<>();

    private LeafReaderContext context;
    private int segmentHits;
    private int totalHits;
    private Scorer scorer;
    private float[] scores;
    private final boolean keepScores;
    private final List<MatchingDocs> matchingDocs = new ArrayList<>();
    private Docs docs;

    /**
     * Default Constructor, does not keep scores.
     */
    public DocValuesCollector()
    {
        this( false );
    }

    /**
     * @param keepScores true if you want to trade correctness for speed
     */
    public DocValuesCollector( boolean keepScores )
    {
        this.keepScores = keepScores;
    }

    /**
     * @param field the field that contains the values
     * @return an iterator over all NumericDocValues from the given field
     */
    public LongValuesIterator getValuesIterator( String field )
    {
        return new LongValuesIterator( getMatchingDocs(), getTotalHits(), field );
    }

    public IndexProgressor getIndexProgressor( String field, IndexProgressor.NodeValueClient client )
    {
        return new LongValuesIndexProgressor( getMatchingDocs(), getTotalHits(), field, client );
    }

    /**
     * @param field the field that contains the values
     * @param sort how the results should be sorted
     * @return an iterator over all NumericDocValues from the given field with respect to the given sort
     * @throws IOException
     */
    public ValuesIterator getSortedValuesIterator( String field, Sort sort ) throws IOException
    {
        if ( sort == null || sort == Sort.INDEXORDER )
        {
            return getValuesIterator( field );
        }
        int size = getTotalHits();
        if ( size == 0 )
        {
            return ValuesIterator.EMPTY;
        }
        TopDocs topDocs = getTopDocs( sort, size );
        LeafReaderContext[] contexts = getLeafReaderContexts( getMatchingDocs() );
        return new TopDocsValuesIterator( topDocs, contexts, field );
    }

    /**
     * Replay the search and collect every hit into TopDocs. One {@code ScoreDoc} is allocated
     * for every hit and the {@code Document} instance is loaded lazily with on every iteration step.
     *
     * @param sort how to sort the iterator. If this is null, results will be in index-order.
     * @return an indexhits iterator over all matches
     * @throws IOException
     */
    public IndexHits<Document> getIndexHits( Sort sort ) throws IOException
    {
        List<MatchingDocs> matchingDocs = getMatchingDocs();
        int size = getTotalHits();
        if ( size == 0 )
        {
            return EMPTY_INDEX_HITS;
        }

        if ( sort == null || sort == Sort.INDEXORDER )
        {
            return new DocsInIndexOrderIterator( matchingDocs, size, isKeepScores() );
        }

        TopDocs topDocs = getTopDocs( sort, size );
        LeafReaderContext[] contexts = getLeafReaderContexts( matchingDocs );
        return new TopDocsIterator( topDocs, contexts );
    }

    /**
     * @return the total number of hits across all segments.
     */
    public int getTotalHits()
    {
        return totalHits;
    }

    /**
     * @return true if scores were saved.
     */
    public boolean isKeepScores()
    {
        return keepScores;
    }

    @Override
    public final void collect( int doc ) throws IOException
    {
        docs.addDoc( doc );
        if ( keepScores )
        {
            if ( segmentHits >= scores.length )
            {
                float[] newScores = new float[ArrayUtil.oversize( segmentHits + 1, 4 )];
                System.arraycopy( scores, 0, newScores, 0, segmentHits );
                scores = newScores;
            }
            scores[segmentHits] = scorer.score();
        }
        segmentHits++;
        totalHits++;
    }

    @Override
    public boolean needsScores()
    {
        return keepScores;
    }

    @Override
    public void setScorer( Scorer scorer )
    {
        this.scorer = scorer;
    }

    @Override
    public void doSetNextReader( LeafReaderContext context )
    {
        if ( docs != null && segmentHits > 0 )
        {
            createMatchingDocs();
        }
        int maxDoc = context.reader().maxDoc();
        docs = createDocs( maxDoc );
        if ( keepScores )
        {
            int initialSize = Math.min( 32, maxDoc );
            scores = new float[initialSize];
        }
        segmentHits = 0;
        this.context = context;
    }

    /**
     * @return the documents matched by the query, one {@link MatchingDocs} per visited segment that contains a hit.
     */
    public List<MatchingDocs> getMatchingDocs()
    {
        if ( docs != null && segmentHits > 0 )
        {
            createMatchingDocs();
            docs = null;
            scores = null;
            context = null;
        }

        return Collections.unmodifiableList( matchingDocs );
    }

    /**
     * @return a new {@link Docs} to record hits.
     */
    private Docs createDocs( final int maxDoc )
    {
        return new Docs( maxDoc );
    }

    private void createMatchingDocs()
    {
        if ( scores == null || scores.length == segmentHits )
        {
            matchingDocs.add( new MatchingDocs( this.context, docs.getDocIdSet(), segmentHits, scores ) );
        }
        else
        {
            // NOTE: we could skip the copy step here since the MatchingDocs are supposed to be
            // consumed through any of the provided Iterators (actually, the replay method),
            // which all don't care if scores has null values at the end.
            // This is for just sanity's sake, we could also make MatchingDocs private
            // and treat this as implementation detail.
            float[] finalScores = new float[segmentHits];
            System.arraycopy( scores, 0, finalScores, 0, segmentHits );
            matchingDocs.add( new MatchingDocs( this.context, docs.getDocIdSet(), segmentHits, finalScores ) );
        }
    }

    private TopDocs getTopDocs( Sort sort, int size ) throws IOException
    {
        TopDocs topDocs;
        if ( sort == Sort.RELEVANCE )
        {
            TopScoreDocCollector collector = TopScoreDocCollector.create( size );
            replayTo( collector );
            topDocs = collector.topDocs();
        }
        else
        {
            TopFieldCollector collector = TopFieldCollector.create( sort, size, false, true, false );
            replayTo( collector );
            topDocs = collector.topDocs();
        }
        return topDocs;
    }

    private static LeafReaderContext[] getLeafReaderContexts( List<MatchingDocs> matchingDocs )
    {
        int segments = matchingDocs.size();
        LeafReaderContext[] contexts = new LeafReaderContext[segments];
        for ( int i = 0; i < segments; i++ )
        {
            MatchingDocs matchingDoc = matchingDocs.get( i );
            contexts[i] = matchingDoc.context;
        }
        return contexts;
    }

    private void replayTo( Collector collector ) throws IOException
    {
        for ( MatchingDocs docs : getMatchingDocs() )
        {
            LeafCollector leafCollector = collector.getLeafCollector( docs.context );
            Scorer scorer;
            DocIdSetIterator idIterator = docs.docIdSet.iterator();
            if ( isKeepScores() )
            {
                scorer = new ReplayingScorer( docs.scores );
            }
            else
            {
                scorer = new ConstantScoreScorer( null, Float.NaN, idIterator );
            }
            leafCollector.setScorer( scorer );
            int doc;
            while ( (doc = idIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS )
            {
                leafCollector.collect( doc );
            }
        }
    }

    /**
     * Iterates over all per-segment {@link DocValuesCollector.MatchingDocs}.
     * Provides base functionality for extracting entity ids and other values from documents.
     */
    private abstract static class LongValuesSource
    {
        private final Iterator<DocValuesCollector.MatchingDocs> matchingDocs;
        private final String field;
        final int totalHits;
        final Map<String,NumericDocValues> docValuesCache;

        DocIdSetIterator currentIdIterator;
        NumericDocValues currentDocValues;
        DocValuesCollector.MatchingDocs currentDocs;
        int index;
        long next;

        LongValuesSource( Iterable<DocValuesCollector.MatchingDocs> allMatchingDocs, int totalHits, String field )
        {
            this.totalHits = totalHits;
            this.field = field;
            matchingDocs = allMatchingDocs.iterator();
            docValuesCache = new HashMap<>();
        }

        /**
         * @return true if it was able to make sure, that currentDisi is valid
         */
        boolean ensureValidDisi()
        {
            try
            {
                while ( currentIdIterator == null )
                {
                    if ( matchingDocs.hasNext() )
                    {
                        currentDocs = matchingDocs.next();
                        currentIdIterator = currentDocs.docIdSet.iterator();
                        if ( currentIdIterator != null )
                        {
                            docValuesCache.clear();
                            currentDocValues = currentDocs.readDocValues( field );
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                return true;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        boolean fetchNextEntityId()
        {
            try
            {
                if ( ensureValidDisi() )
                {
                    int nextDoc = currentIdIterator.nextDoc();
                    if ( nextDoc != DocIdSetIterator.NO_MORE_DOCS )
                    {
                        index++;
                        next = currentDocValues.get( nextDoc );
                        return true;
                    }
                    else
                    {
                        currentIdIterator = null;
                        return fetchNextEntityId();
                    }
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }

            return false;
        }
    }

    /**
     * Iterates over all per-segment {@link DocValuesCollector.MatchingDocs}. Supports two kinds of lookups.
     * One, iterate over all long values of the given field (constructor argument).
     * Two, lookup a value for the current doc in a sidecar {@code NumericDocValues} field.
     * That is, this iterator has a main field, that drives the iteration and allow for lookups
     * in other, secondary fields based on the current document of the main iteration.
     *
     * Lookups from this class are not thread-safe. Races can happen when the segment barrier
     * is crossed; one thread might think it is reading from one segment while another thread has
     * already advanced this Iterator to the next segment, having raced the first thread.
     */
    public static class LongValuesIterator extends LongValuesSource implements ValuesIterator, PrimitiveLongResourceIterator
    {
        private boolean hasNext;
        private boolean hasNextDecided;

        /**
         * @param allMatchingDocs all {@link DocValuesCollector.MatchingDocs} across all segments
         * @param totalHits the total number of hits across all segments
         * @param field the main field, whose values drive the iteration
         */
        public LongValuesIterator( Iterable<DocValuesCollector.MatchingDocs> allMatchingDocs, int totalHits, String field )
        {
            super( allMatchingDocs, totalHits, field );
        }

        @Override
        public long current()
        {
            return next;
        }

        @Override
        public float currentScore()
        {
            return 0;
        }

        @Override
        public long getValue( String field )
        {
            if ( ensureValidDisi() )
            {
                if ( docValuesCache.containsKey( field ) )
                {
                    return docValuesCache.get( field ).get( currentIdIterator.docID() );
                }

                NumericDocValues docValues = currentDocs.readDocValues( field );
                docValuesCache.put( field, docValues );

                return docValues.get( currentIdIterator.docID() );
            }
            else
            {
                // same as DocValues.emptyNumeric()#get
                // which means, getValue carries over the semantics of NDV
                // -1 would also be a possibility here.
                return 0;
            }
        }

        @Override
        public boolean hasNext()
        {
            if ( !hasNextDecided )
            {
                hasNext = fetchNextEntityId();
                hasNextDecided = true;
            }
            return hasNext;
        }

        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            hasNextDecided = false;
            return next;
        }

        @Override
        public int remaining()
        {
            return totalHits - index;
        }

        @Override
        public void close()
        {
            // nothing to close
        }
    }

    private static class LongValuesIndexProgressor extends LongValuesSource implements IndexProgressor
    {
        private final NodeValueClient client;

        LongValuesIndexProgressor( Iterable<MatchingDocs> allMatchingDocs, int totalHits, String field, NodeValueClient client )
        {
            super( allMatchingDocs, totalHits, field );
            this.client = client;
        }

        @Override
        public boolean next()
        {
            while ( fetchNextEntityId() )
            {
                if ( client.acceptNode( next, (Value[]) null ) )
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close()
        {
            // nothing to close
        }
    }

    /**
     * Holds the documents that were matched per segment.
     */
    static final class MatchingDocs
    {

        /** The {@code LeafReaderContext} for this segment. */
        public final LeafReaderContext context;

        /** Which documents were seen. */
        public final DocIdSet docIdSet;

        /** Non-sparse scores array. Might be null of no scores were required. */
        public final float[] scores;

        /** Total number of hits */
        public final int totalHits;

        MatchingDocs( LeafReaderContext context, DocIdSet docIdSet, int totalHits, float[] scores )
        {
            this.context = context;
            this.docIdSet = docIdSet;
            this.totalHits = totalHits;
            this.scores = scores;
        }

        /**
         * @return the {@code NumericDocValues} for a given field
         * @throws IllegalArgumentException if this field is not indexed with numeric doc values
         */
        public NumericDocValues readDocValues( String field )
        {
            try
            {
                NumericDocValues dv = context.reader().getNumericDocValues( field );
                if ( dv == null )
                {
                    FieldInfo fi = context.reader().getFieldInfos().fieldInfo( field );
                    DocValuesType actual = null;
                    if ( fi != null )
                    {
                        actual = fi.getDocValuesType();
                    }
                    throw new IllegalStateException(
                            "The field '" + field + "' is not indexed properly, expected NumericDV, but got '" +
                            actual + "'" );
                }
                return dv;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    /**
     * Used during collection to record matching docs and then return a
     * {@see DocIdSet} that contains them.
     */
    private static final class Docs
    {
        private final DocIdSetBuilder bits;

        Docs( int maxDoc )
        {
            bits = new DocIdSetBuilder( maxDoc );
        }

        /** Record the given document. */
        public void addDoc( int docId )
        {
            bits.add( docId );
        }

        /** Return the {@see DocIdSet} which contains all the recorded docs. */
        public DocIdSet getDocIdSet()
        {
            return bits.build();
        }
    }

    private static class ReplayingScorer extends Scorer
    {

        private final float[] scores;
        private int index;

        ReplayingScorer( float[] scores )
        {
            super( null );
            this.scores = scores;
        }

        @Override
        public float score()
        {
            if ( index < scores.length )
            {
                return scores[index++];
            }
            return Float.NaN;
        }

        @Override
        public int freq()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocIdSetIterator iterator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID()
        {
            throw new UnsupportedOperationException();
        }

    }

    private static final class DocsInIndexOrderIterator extends AbstractIndexHits<Document>
    {
        private final Iterator<MatchingDocs> docs;
        private final int size;
        private final boolean keepScores;
        private DocIdSetIterator currentIdIterator;
        private Scorer currentScorer;
        private LeafReader currentReader;

        private DocsInIndexOrderIterator( List<MatchingDocs> docs, int size, boolean keepScores )
        {
            this.size = size;
            this.keepScores = keepScores;
            this.docs = docs.iterator();
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public float currentScore()
        {
            try
            {
                return currentScorer.score();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        protected Document fetchNextOrNull()
        {
            if ( ensureValidDisi() )
            {
                try
                {
                    int doc = currentIdIterator.nextDoc();
                    if ( doc == DocIdSetIterator.NO_MORE_DOCS )
                    {
                        currentIdIterator = null;
                        currentScorer = null;
                        currentReader = null;
                        return fetchNextOrNull();
                    }
                    return currentReader.document( doc );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
            else
            {
                return null;
            }
        }

        private boolean ensureValidDisi()
        {
            while ( currentIdIterator == null && docs.hasNext() )
            {
                MatchingDocs matchingDocs = docs.next();
                try
                {
                    currentIdIterator = matchingDocs.docIdSet.iterator();
                    if ( keepScores )
                    {
                        currentScorer = new ReplayingScorer( matchingDocs.scores );
                    }
                    else
                    {
                        currentScorer = new ConstantScoreScorer( null, Float.NaN, currentIdIterator );
                    }
                    currentReader = matchingDocs.context.reader();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
            return currentIdIterator != null;
        }
    }

    private abstract static class ScoreDocsIterator extends PrefetchingIterator<ScoreDoc>
    {
        private final Iterator<ScoreDoc> iterator;
        private final int[] docStarts;
        private final LeafReaderContext[] contexts;
        protected ScoreDoc currentDoc;

        private ScoreDocsIterator( TopDocs docs, LeafReaderContext[] contexts )
        {
            this.contexts = contexts;
            this.iterator = new ArrayIterator<>( docs.scoreDocs );
            int segments = contexts.length;
            docStarts = new int[segments + 1];
            for ( int i = 0; i < segments; i++ )
            {
                LeafReaderContext context = contexts[i];
                docStarts[i] = context.docBase;
            }
            LeafReaderContext lastContext = contexts[segments - 1];
            docStarts[segments] = lastContext.docBase + lastContext.reader().maxDoc();
        }

        public ScoreDoc getCurrentDoc()
        {
            return currentDoc;
        }

        @Override
        protected ScoreDoc fetchNextOrNull()
        {
            if ( !iterator.hasNext() )
            {
                return null;
            }
            currentDoc = iterator.next();
            int subIndex = ReaderUtil.subIndex( currentDoc.doc, docStarts );
            LeafReaderContext context = contexts[subIndex];
            onNextDoc( currentDoc.doc - context.docBase, context );
            return currentDoc;
        }

        protected abstract void onNextDoc( int localDocID, LeafReaderContext context );
    }

    private static final class TopDocsIterator extends AbstractIndexHits<Document>
    {
        private final int size;
        private final ScoreDocsIterator scoreDocs;
        private Document currentDoc;

        private TopDocsIterator( TopDocs docs, LeafReaderContext[] contexts )
        {
            scoreDocs = new ScoreDocsIterator( docs, contexts )
            {
                @Override
                protected void onNextDoc( int localDocID, LeafReaderContext context )
                {
                    updateCurrentDocument( localDocID, context.reader() );
                }
            };
            this.size = docs.scoreDocs.length;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public float currentScore()
        {
            return scoreDocs.getCurrentDoc().score;
        }

        @Override
        protected Document fetchNextOrNull()
        {
            if ( !scoreDocs.hasNext() )
            {
                return null;
            }
            scoreDocs.next();
            return currentDoc;
        }

        private void updateCurrentDocument( int docID, LeafReader reader )
        {
            try
            {
                currentDoc = reader.document( docID );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static final class TopDocsValuesIterator extends ValuesIterator.Adapter
    {
        private final ScoreDocsIterator scoreDocs;
        private final String field;
        private final Map<LeafReaderContext,NumericDocValues> docValuesCache;
        private final Map<String,NumericDocValues> readerCache;
        private long currentValue;
        private LeafReaderContext currentContext;
        private int currentDocID;

        TopDocsValuesIterator( TopDocs docs, LeafReaderContext[] contexts, String field )
        {
            super( docs.totalHits );
            this.field = field;
            docValuesCache = new HashMap<>( contexts.length );
            readerCache = new HashMap<>();
            scoreDocs = new ScoreDocsIterator( docs, contexts )
            {
                @Override
                protected void onNextDoc( int localDocID, LeafReaderContext context )
                {
                    readerCache.clear();
                    currentContext = context;
                    currentDocID = localDocID;
                    loadNextValue( context, localDocID );
                }
            };
        }

        @Override
        protected boolean fetchNext()
        {
            if ( scoreDocs.hasNext() )
            {
                scoreDocs.next();
                index++;
                return currentValue != -1 && next( currentValue );
            }
            return false;
        }

        @Override
        public long current()
        {
            return index;
        }

        @Override
        public float currentScore()
        {
            return scoreDocs.getCurrentDoc().score;
        }

        @Override
        public long getValue( String field )
        {
            NumericDocValues fieldValues = readerCache.computeIfAbsent( field, this::getDocValues );
            return fieldValues.get( currentDocID );
        }

        private NumericDocValues getDocValues( String field )
        {
            try
            {
                return currentContext.reader().getNumericDocValues( field );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Fail to read numeric doc values field " + field + " from the document.", e );
            }
        }

        private void loadNextValue( LeafReaderContext context, int docID )
        {
            NumericDocValues docValues;
            if ( docValuesCache.containsKey( context ) )
            {
                docValues = docValuesCache.get( context );
            }
            else
            {
                try
                {
                    docValues = context.reader().getNumericDocValues( field );
                    docValuesCache.put( context, docValues );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
            if ( docValues != null )
            {
                currentValue = docValues.get( docID );
            }
            else
            {
                currentValue = -1;
            }
        }
    }
}
