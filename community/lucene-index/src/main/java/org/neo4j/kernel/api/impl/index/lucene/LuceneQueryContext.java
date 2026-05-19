/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.values.storable.Value;

/**
 * A generic representation of a Lucene query that will defer instantiation of
 * internal Lucene classes until the version is known.
 */
public interface LuceneQueryContext {

    /**
     * Add a query term that must occur.
     *
     * @param field Field in the document.
     * @param text Text of the field that must occur.
     */
    LuceneQueryContext addMustTerm(String field, String text);

    /**
     * Add a query term that must occur, using a constant score.
     *
     * @param field Field in the document.
     * @param text Text of the field that must occur.
     */
    LuceneQueryContext addConstantMustTerm(String field, String text);

    /**
     * Add a query term to exclude results containing a specified field.
     *
     * @param field Field name to match on.
     */
    LuceneQueryContext addMustNotHaveField(String field);

    /**
     * Create a query from a "Lucene query"
     *
     * @param query A Lucene string query
     * @param fields The fields available to query.
     * @param analyzer The analyzer to use when parsing.
     * @throws LuceneQueryParseException If the provided query could not be parsed.
     */
    LuceneQueryContext addShouldQueryText(String query, String[] fields, Analyzer analyzer)
            throws LuceneQueryParseException;

    /**
     * Query a filed for an exact value.
     *
     * @param entityIdKey Name of field to search.
     * @param entityId The exact value to search for.
     */
    LuceneQueryContext exactTerm(String entityIdKey, long entityId);

    /**
     * Add an exact trigram term that must occur.
     *
     * @param value String to do a trigram search over.
     */
    LuceneQueryContext addExactTrigram(String value);

    /**
     * Add a seek query that must contain the provided values
     * @param propertyValues
     * @return
     */
    LuceneQueryContext addMustSeek(Value... propertyValues);

    /**
     * A query that will match all documents in the segment.
     */
    LuceneQueryContext matchAll();

    /**
     * A query that will do a prefix match on a string field.
     *
     * @param prefix Prefix to search for.
     */
    LuceneQueryContext stringPrefix(String prefix);

    /**
     * A query that will do a contains match on a string field.
     *
     * @param substring Substring to search for.
     */
    LuceneQueryContext stringContains(String substring);

    /**
     * A query that will do a suffix match on a string field.
     *
     * @param suffix Suffix to search for.
     */
    LuceneQueryContext stringSuffix(String suffix);

    /**
     * A query that will do a trigram search on a string field.
     *
     * @param searchString String to do a trigram search over.
     */
    LuceneQueryContext trigramSearch(String searchString);

    /**
     * An approximate KNN search.
     *
     * @param documentStructure The documents structure to use.
     * @param query Vector to search around.
     * @param k Number of documents to find.
     * @param efSearch Number of nearest neighbors to consider. Must be >= `k`.
     * @param rescore If the query should rescore the results
     */
    LuceneQueryContext approximateNearestNeighbors(
            VectorDocumentStructure documentStructure, float[] query, int k, int efSearch, boolean rescore);

    /**
     * An approximate KNN search optional rescoring and with filters.
     *
     * @param documentStructure The documents structure to use.
     * @param query Vector to search around.
     * @param k Number of documents to find.
     * @param efSearch Number of nearest neighbors to consider. Must be >= `k`.
     * @param rescore If the query should rescore the results
     * @param entityFilter an entityFilter to .
     * @param filterQueries the queries with which to filter the search.
     */
    LuceneQueryContext approximateNearestNeighbors(
            VectorDocumentStructure documentStructure,
            float[] query,
            int k,
            int efSearch,
            boolean rescore,
            EntityFilterPredicate entityFilter,
            PropertyIndexQuery... filterQueries);
}
