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
package org.neo4j.kernel.api.impl.schema.trigram;

import static org.neo4j.kernel.api.impl.schema.trigram.TrigramDocumentStructure.TRIGRAM_VALUE_KEY;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramTokenStream.CodePointBuffer;

class TrigramQueryFactory {
    static Query getById(long entityId) {
        var term = TrigramDocumentStructure.newTermForChangeOrRemove(entityId);
        return new TermQuery(term);
    }

    // Need to filter out false positives
    static Query exact(String value) {
        return trigramSearch(value);
    }

    // Need to filter out false positives
    static Query stringPrefix(String prefix) {
        return trigramSearch(prefix);
    }

    // Need to filter out false positives
    static Query stringContains(String contains) {
        return trigramSearch(contains);
    }

    // Need to filter out false positives
    static Query stringSuffix(String suffix) {
        return trigramSearch(suffix);
    }

    static MatchAllDocsQuery allValues() {
        return new MatchAllDocsQuery();
    }

    static boolean needStoreFilter(PropertyIndexQuery predicate) {
        return !predicate.type().equals(IndexQueryType.ALL_ENTRIES);
    }

    private static Query trigramSearch(String searchString) {
        if (searchString.isEmpty()) {
            return allValues();
        }

        var codePointBuffer = TrigramTokenStream.getCodePoints(searchString);

        if (codePointBuffer.codePointCount() < 3) {
            String searchTerm = QueryParser.escape(searchString);
            Term term = new Term(TRIGRAM_VALUE_KEY, "*" + searchTerm + "*");
            return new WildcardQuery(term);
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // Don't generate more clauses than what is allowed by IndexSearcher.
        // Default value for IndexSearcher.getMaxClauseCount() is 1024 which is assumed to be enough to not generate too
        // many false positives. And those false positives will be filtered out later as usual.
        for (int i = 0; i < codePointBuffer.codePointCount() - 2 && i < IndexSearcher.getMaxClauseCount(); i++) {
            String term = getNgram(codePointBuffer, i, 3);

            var termQuery = new ConstantScoreQuery(new TermQuery(new Term(TRIGRAM_VALUE_KEY, term)));
            builder.add(termQuery, BooleanClause.Occur.MUST);
        }
        return builder.build();
    }

    private static String getNgram(CodePointBuffer codePointBuffer, int ngramIndex, int n) {
        char[] termCharBuffer = new char[2 * n];
        int length = CharacterUtils.toChars(codePointBuffer.codePoints(), ngramIndex, n, termCharBuffer, 0);
        return new String(termCharBuffer, 0, length);
    }
}
