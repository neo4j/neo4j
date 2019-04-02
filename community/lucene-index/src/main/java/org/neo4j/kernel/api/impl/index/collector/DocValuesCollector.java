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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.util.VisibleForTesting;
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
    private LeafReaderContext context;
    private int segmentHits;
    private int totalHits;
    private Scorable scorer;
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

    public IndexProgressor getIndexProgressor( String field, IndexProgressor.EntityValueClient client )
    {
        return new LongValuesIndexProgressor( getMatchingDocs(), getTotalHits(), field, client );
    }

    /**
     * @param field the field that contains the values
     * @return an iterator over all NumericDocValues from the given field, in highest-to-lowest relevance order.
     * @throws IOException if an exception occurs while querying the index.
     */
    public ValuesIterator getValuesSortedByRelevance( String field ) throws IOException
    {
        int size = getTotalHits();
        if ( size == 0 )
        {
            return ValuesIterator.EMPTY;
        }
        TopDocs topDocs = getTopDocsByRelevance( size );
        LeafReaderContext[] contexts = getLeafReaderContexts( getMatchingDocs() );
        return new TopDocsValuesIterator( topDocs, contexts, field );
    }

    /**
     * @return the total number of hits across all segments.
     */
    int getTotalHits()
    {
        return totalHits;
    }

    /**
     * @return true if scores were saved.
     */
    private boolean isKeepScores()
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
    public ScoreMode scoreMode()
    {
        return keepScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void setScorer( Scorable scorer )
    {
        this.scorer = scorer;
    }

    @Override
    public void doSetNextReader( LeafReaderContext context ) throws IOException
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
    @VisibleForTesting
    List<MatchingDocs> getMatchingDocs()
    {
        if ( docs != null && segmentHits > 0 )
        {
            try
            {
                createMatchingDocs();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            finally
            {
                docs = null;
                scores = null;
                context = null;
            }
        }

        return matchingDocs;
    }

    /**
     * @return a new {@link Docs} to record hits.
     */
    private Docs createDocs( final int maxDoc )
    {
        return new Docs( maxDoc );
    }

    private void createMatchingDocs() throws IOException
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

    private TopDocs getTopDocsByRelevance( int size ) throws IOException
    {
        TopScoreDocCollector collector = TopScoreDocCollector.create( size, size );
        replayTo( collector );
        return collector.topDocs();
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
            DocIdSetIterator idIterator = docs.docIdSet;
            Weight weight = new Weight( null )
            {
                @Override
                public void extractTerms( Set<Term> terms )
                {
                }

                @Override
                public Explanation explain( LeafReaderContext context, int doc )
                {
                    return null;
                }

                @Override
                public Scorer scorer( LeafReaderContext context )
                {
                    return null;
                }

                @Override
                public boolean isCacheable( LeafReaderContext ctx )
                {
                    return false;
                }
            };
            if ( isKeepScores() )
            {
                scorer = new ReplayingScorer( weight, docs.scores );
            }
            else
            {
                scorer = new ConstantScoreScorer( weight, Float.NaN, scoreMode(), idIterator );
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

        DocIdSetIterator currentIdIterator;
        NumericDocValues currentDocValues;
        DocValuesCollector.MatchingDocs currentDocs;
        float score;
        int index;
        long next;

        LongValuesSource( Iterable<DocValuesCollector.MatchingDocs> allMatchingDocs, int totalHits, String field )
        {
            this.totalHits = totalHits;
            this.field = field;
            matchingDocs = allMatchingDocs.iterator();
            score = Float.NaN;
        }

        /**
         * @return true if it was able to make sure, that currentDisi is valid
         */
        boolean ensureValidDisi()
        {
            while ( currentIdIterator == null )
            {
                if ( matchingDocs.hasNext() )
                {
                    currentDocs = matchingDocs.next();
                    currentIdIterator = currentDocs.docIdSet;
                    if ( currentIdIterator != null )
                    {
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

        boolean fetchNextEntityId()
        {
            try
            {
                if ( ensureValidDisi() )
                {
                    int nextDoc = currentIdIterator.nextDoc();
                    if ( nextDoc != DocIdSetIterator.NO_MORE_DOCS )
                    {
                        if ( currentDocs.scores != null )
                        {
                            score = currentDocs.scores[index];
                        }
                        index++;
                        int valueDoc = currentDocValues.advance( nextDoc );
                        if ( valueDoc != nextDoc )
                        {
                            throw new RuntimeException( "Document id and document value iterators are out of sync. Id iterator gave me document " + nextDoc +
                                    ", while the value iterator gave me document " + valueDoc + "." );
                        }
                        next = currentDocValues.longValue();
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

    private static class LongValuesIndexProgressor extends LongValuesSource implements IndexProgressor
    {
        private final EntityValueClient client;

        LongValuesIndexProgressor( Iterable<MatchingDocs> allMatchingDocs, int totalHits, String field, EntityValueClient client )
        {
            super( allMatchingDocs, totalHits, field );
            this.client = client;
        }

        @Override
        public boolean next()
        {
            while ( fetchNextEntityId() )
            {
                if ( client.acceptEntity( next, score, (Value[]) null ) )
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
        final DocIdSetIterator docIdSet;

        /** Non-sparse scores array. Might be null of no scores were required. */
        final float[] scores;

        /** Total number of hits */
        final int totalHits;

        MatchingDocs( LeafReaderContext context, DocIdSetIterator docIdSet, int totalHits, float[] scores )
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
        private NumericDocValues readDocValues( String field )
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
        private void addDoc( int docId )
        {
            bits.grow( 1 ).add( docId );
        }

        /** Return the {@see DocIdSet} which contains all the recorded docs. */
        private DocIdSetIterator getDocIdSet() throws IOException
        {
            return bits.build().iterator();
        }
    }

    private static class ReplayingScorer extends Scorer
    {
        private final float[] scores;
        private int index;

        ReplayingScorer( Weight weight, float[] scores )
        {
            super( weight );
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
        public DocIdSetIterator iterator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getMaxScore( int upTo )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID()
        {
            throw new UnsupportedOperationException();
        }

    }

    private abstract static class ScoreDocsIterator extends PrefetchingIterator<ScoreDoc>
    {
        private final Iterator<ScoreDoc> iterator;
        private final int[] docStarts;
        private final LeafReaderContext[] contexts;
        private ScoreDoc currentDoc;

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

        private ScoreDoc getCurrentDoc()
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

    private static final class TopDocsValuesIterator extends ValuesIterator.Adapter
    {
        private final ScoreDocsIterator scoreDocs;
        private final String field;
        private final Map<LeafReaderContext,NumericDocValues> docValuesCache;
        private long currentValue;

        TopDocsValuesIterator( TopDocs docs, LeafReaderContext[] contexts, String field )
        {
            super( Math.toIntExact( docs.totalHits.value ) );
            if ( docs.totalHits.relation != TotalHits.Relation.EQUAL_TO )
            {
                throw new RuntimeException( "Expected total hits value to be exact (EQUAL_TO), but it was: " + docs.totalHits.relation );
            }
            this.field = field;
            docValuesCache = new HashMap<>( contexts.length );
            scoreDocs = new ScoreDocsIterator( docs, contexts )
            {
                @Override
                protected void onNextDoc( int localDocID, LeafReaderContext context )
                {
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
            return currentValue;
        }

        @Override
        public float currentScore()
        {
            return scoreDocs.getCurrentDoc().score;
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
                try
                {
                    int valueDocId = docValues.advance( docID );
                    if ( valueDocId != docID )
                    {
                        throw new RuntimeException( "Expected doc values and doc scores to iterate together, but score doc id is " + docID +
                                ", and value doc id is " + valueDocId + "." );
                    }
                    currentValue = docValues.longValue();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
            else
            {
                currentValue = -1;
            }
        }
    }
}
