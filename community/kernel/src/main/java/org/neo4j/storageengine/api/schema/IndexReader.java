/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.storageengine.api.schema;


import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Resource;


/**
 * Reader for an index. Must honor repeatable reads, which means that if a lookup is executed multiple times the
 * same result set must be returned.
 */
public interface IndexReader extends Resource
{
    /**
     * Searches this index for a certain value.
     *
     * @param value property value to search for.
     * @return ids of matching nodes.
     */
    PrimitiveLongIterator seek( Object value );

    /**
     * Searches this index for numerics values between {@code lower} and {@code upper}.
     *
     * @param lower lower numeric bound of search (inclusive).
     * @param upper upper numeric bound of search (inclusive).
     * @return ids of matching nodes.
     */
    PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper );

    /**
     * Searches this index for string values between {@code lower} and {@code upper}.
     *
     * @param lower lower numeric bound of search.
     * @param includeLower whether or not lower bound is inclusive.
     * @param upper upper numeric bound of search.
     * @param includeUpper whether or not upper bound is inclusive.
     * @return ids of matching nodes.
     */
    PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower, String upper, boolean includeUpper );

    /**
     * Searches this index for string values starting with {@code prefix}.
     *
     * @param prefix prefix that matching strings must start with.
     * @return ids of matching nodes.
     */
    PrimitiveLongIterator rangeSeekByPrefix( String prefix );

    /**
     * Scans this index returning all nodes.
     *
     * @return node ids in index.
     */
    PrimitiveLongIterator scan();

    /**
     * Searches this index for string values containing the exact search string.
     *
     * @param exactTerm the exact string to search for in the index
     * @return ids of matching nodes.
     */
    PrimitiveLongIterator containsString( String exactTerm );

    /**
     * Searches this index for string values ending with the suffix search string.
     *
     * @param suffix the string to search for in the index
     * @return ids of matching nodes.
     */
    PrimitiveLongIterator endsWith( String suffix );

    /**
     * @param nodeId node if to match.
     * @param propertyValue property value to match.
     * @return number of index entries for the given {@code nodeId} and {@code propertyValue}.
     */
    long countIndexedNodes( long nodeId, Object propertyValue );

    IndexSampler createSampler();

    IndexReader EMPTY = new IndexReader()
    {
        @Override
        public PrimitiveLongIterator seek( Object value )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower,
                                                        String upper, boolean includeUpper )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public PrimitiveLongIterator rangeSeekByPrefix( String prefix )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public PrimitiveLongIterator scan()
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public PrimitiveLongIterator containsString( String exactTerm )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public PrimitiveLongIterator endsWith(String suffix)
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        // Used for checking index correctness
        @Override
        public long countIndexedNodes( long nodeId, Object propertyValue )
        {
            return 0;
        }

        @Override
        public IndexSampler createSampler()
        {
            return IndexSampler.EMPTY;
        }

        @Override
        public void close()
        {
        }
    };
}
