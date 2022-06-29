/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.impl.schema.trigram;

import static org.neo4j.kernel.api.impl.schema.trigram.TrigramDocumentStructure.TRIGRAM_VALUE_KEY;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.neo4j.values.storable.Value;

class TrigramQueryFactory {
    static Query getById(long entityId) {
        var term = new Term(TrigramDocumentStructure.ENTITY_ID_KEY, "" + entityId);
        return new TermQuery(term);
    }

    static Query exact(Value value) {
        return trigramSearch(value.asObject().toString());
    }

    static Query stringPrefix(String prefix) {
        return trigramSearch(prefix);
    }

    static Query stringContains(String contains) {
        return trigramSearch(contains);
    }

    static Query stringSuffix(String suffix) {
        return trigramSearch(suffix);
    }

    static MatchAllDocsQuery allValues() {
        return new MatchAllDocsQuery();
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

        for (int i = 0; i < codePointBuffer.codePointCount() - 2; i++) {
            char[] termCharBuffer = new char[6];
            int length = CharacterUtils.toChars(codePointBuffer.codePoints(), i, 3, termCharBuffer, 0);
            String term = new String(termCharBuffer, 0, length);

            var termQuery = new ConstantScoreQuery(new TermQuery(new Term(TRIGRAM_VALUE_KEY, term)));
            builder.add(termQuery, BooleanClause.Occur.MUST);
        }
        return builder.build();
    }
}
