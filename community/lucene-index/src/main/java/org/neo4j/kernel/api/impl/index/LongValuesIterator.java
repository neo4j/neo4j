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

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;

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
public class LongValuesIterator extends PrimitiveLongBaseIterator implements DocValuesAccess
{
    private final Iterator<DocValuesCollector.MatchingDocs> matchingDocs;
    private final String field;
    private final int size;
    private DocIdSetIterator currentDisi;
    private NumericDocValues currentDocValues;
    private DocValuesCollector.MatchingDocs currentDocs;
    private final Map<String,NumericDocValues> docValuesCache;

    private int index = 0;

    /**
     * @param allMatchingDocs all {@link DocValuesCollector.MatchingDocs} across all segments
     * @param totalHits the total number of hits across all segments
     * @param field the main field, whose values drive the iteration
     */
    public LongValuesIterator( Iterable<DocValuesCollector.MatchingDocs> allMatchingDocs, int totalHits, String field )
    {
        this.size = totalHits;
        this.field = field;
        matchingDocs = allMatchingDocs.iterator();
        docValuesCache = new HashMap<>();
    }

    /**
     * @return the number of docs left in this iterator.
     */
    public int remaining()
    {
        return size - index;
    }

    @Override
    public long current()
    {
        return next;
    }

    @Override
    public long getValue( String field )
    {
        if ( ensureValidDisi() )
        {
            if ( docValuesCache.containsKey( field ) )
            {
                return docValuesCache.get( field ).get( currentDisi.docID() );
            }

            NumericDocValues docValues = currentDocs.readDocValues( field );
            docValuesCache.put( field, docValues );

            return docValues.get( currentDisi.docID() );
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
    protected boolean fetchNext()
    {
        try
        {
            if ( ensureValidDisi() )
            {
                int nextDoc = currentDisi.nextDoc();
                if ( nextDoc != DocIdSetIterator.NO_MORE_DOCS )
                {
                    index++;
                    return next( currentDocValues.get( nextDoc ) );
                }
                else
                {
                    currentDisi = null;
                    return fetchNext();
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return false;
    }

    /**
     * @return true if it was able to make sure, that currentDisi is valid
     */
    private boolean ensureValidDisi()
    {
        try
        {
            while ( currentDisi == null )
            {
                if ( matchingDocs.hasNext() )
                {
                    currentDocs = matchingDocs.next();
                    currentDisi = currentDocs.docIdSet.iterator();
                    if ( currentDisi != null )
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
}
