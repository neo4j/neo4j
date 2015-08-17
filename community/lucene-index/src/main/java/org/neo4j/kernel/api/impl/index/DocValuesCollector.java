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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.RoaringDocIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Collector to record per-segment {@code DocIdSet}s and {@code LeafReaderContext}s for every
 * segment that contains a hit. Those items can be later used to read {@code DocValues} fields
 * and iterate over the matched {@code DocIdSet}s. This collector is different from
 * {@code org.apache.lucene.search.CachingCollector} in that the later focuses on predictable RAM usage
 * and feeding other collectors while this collector focuses on exposing the required per-segment data structures
 * to the user.
 */
public final class DocValuesCollector extends SimpleCollector
{

    private LeafReaderContext context;
    private int segmentHits;
    private int totalHits;
    private final List<MatchingDocs> matchingDocs = new ArrayList<>();
    private Docs docs;

    /**
     * @return the documents matched by the query, one {@link MatchingDocs} per visited segment that contains a hit.
     */
    public List<MatchingDocs> getMatchingDocs()
    {
        if ( docs != null && segmentHits > 0 )
        {
            matchingDocs.add( new MatchingDocs( this.context, docs.getDocIdSet(), segmentHits ) );
            docs = null;
            context = null;
        }

        return Collections.unmodifiableList( matchingDocs );
    }

    /**
     * @return the total number of hits across all segments.
     */
    public int getTotalHits()
    {
        return totalHits;
    }

    @Override
    public final void collect( int doc ) throws IOException
    {
        docs.addDoc( doc );
        segmentHits++;
        totalHits++;
    }

    @Override
    public boolean needsScores()
    {
        return false;
    }

    @Override
    protected void doSetNextReader( LeafReaderContext context ) throws IOException
    {
        if ( docs != null && segmentHits > 0 )
        {
            matchingDocs.add( new MatchingDocs( this.context, docs.getDocIdSet(), segmentHits ) );
        }
        docs = createDocs( context.reader().maxDoc() );
        segmentHits = 0;
        this.context = context;
    }

    /**
     * @return a new {@link Docs} to record hits.
     */
    private Docs createDocs( final int maxDoc )
    {
        return new Docs( maxDoc );
    }

    /**
     * Holds the documents that were matched per segment.
     */
    public final static class MatchingDocs
    {

        /** The {@code LeafReaderContext} for this segment. */
        public final LeafReaderContext context;

        /** Which documents were seen. */
        public final DocIdSet docIdSet;

        /** Total number of hits */
        public final int totalHits;

        public MatchingDocs( LeafReaderContext context, DocIdSet docIdSet, int totalHits )
        {
            this.context = context;
            this.docIdSet = docIdSet;
            this.totalHits = totalHits;
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
        private final RoaringDocIdSet.Builder bits;

        public Docs( int maxDoc )
        {
            bits = new RoaringDocIdSet.Builder( maxDoc );
        }

        /** Record the given document. */
        public void addDoc( int docId )
        {
            bits.add( docId );
        }

        ;

        /** Return the {@see DocIdSet} which contains all the recorded docs. */
        public DocIdSet getDocIdSet()
        {
            return bits.build();
        }
    }
}
