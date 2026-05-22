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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static org.apache.lucene.util.automaton.Automata.makeBinaryStringUnion;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.eclipse.collections.api.set.primitive.LongSet;

final class EntityIdSetQuery extends Query {
    private final String field;
    private final LongSet set;

    static EntityIdSetQuery create(String field, LongSet set) {
        return new EntityIdSetQuery(field, set);
    }

    private EntityIdSetQuery(String field, LongSet set) {
        this.field = field;
        this.set = set;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (set.isEmpty()) {
            return MatchNoDocsQuery.INSTANCE;
        } else if (set.size() * 20L < searcher.getIndexReader().maxDoc()) {
            // From benchmarks we see that when the size of the set is <= 5%
            // of the size of the index, the AutomatonQuery is faster
            SortedSet<BytesRef> idStrings = new TreeSet<>();
            set.forEach(id -> idStrings.add(new BytesRef(Long.toString(id))));
            return new AutomatonQuery(new Term(field), makeBinaryStringUnion(idStrings), true);
        } else {
            return super.rewrite(searcher); // keep EntityIdSetQuery path
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                NumericDocValues idValues = context.reader().getNumericDocValues(field);
                if (idValues == null) {
                    return null;
                }

                DocIdSetIterator iterator = new FilteredDocIdSetIterator(idValues) {
                    @Override
                    protected boolean match(int doc) throws IOException {
                        return set.contains(idValues.longValue());
                    }
                };

                return new DefaultScorerSupplier(new ConstantScoreScorer(1f, scoreMode, iterator));
            }
        };
    }

    @Override
    public String toString(String field) {
        return field + ":<entity-in-set>";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    // NOTE: Use referential equality since we are not using the query-cache anyway,
    //      rather than using the O(N) equals of the LongSet.
    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    // NOTE: Use referential equality since we are not using the query-cache anyway,
    //      rather than using the O(N) equals of the LongSet.
    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
