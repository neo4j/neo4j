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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashSet;
import java.util.function.Function;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.kernel.impl.api.LuceneIndexValueValidator;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class TrigramQueryFactoryTest {
    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @EnumSource
    void shouldHandleLargeSearchStrings(TrigramQuery trigramQuery) {
        // Given
        var size = LuceneIndexValueValidator.MAX_TERM_LENGTH;

        // When
        var query = trigramQuery.query(random.nextAlphaNumericString(size, size));

        // Then
        var terms = new HashSet<Term>();
        query.visit(QueryVisitor.termCollector(terms));
        assertThat(terms.size()).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(IndexSearcher.getMaxClauseCount());
    }

    private enum TrigramQuery {
        EXACT(s -> TrigramQueryFactory.exact(s)),
        PREFIX(s -> TrigramQueryFactory.stringPrefix(s)),
        SUFFIX(s -> TrigramQueryFactory.stringSuffix(s)),
        CONTAINS(s -> TrigramQueryFactory.stringContains(s));

        final Function<String, Query> queryFunction;

        TrigramQuery(Function<String, Query> queryFunction) {
            this.queryFunction = queryFunction;
        }

        Query query(String searchString) {
            return queryFunction.apply(searchString);
        }
    }
}
