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
package org.neo4j.kernel.api.impl.schema.reader;

import java.io.IOException;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.neo4j.kernel.api.impl.schema.ValueEncoding;

/**
 * Lucene queries for text queries using Cypher text operation semantics
 * instead of fuzzy fulltext search.
 * <p>
 * These operations can be used only for Lucene
 * indexes created using {@link org.apache.lucene.analysis.core.KeywordAnalyzer}.
 * The operations are optimised to be used to query such indexes and perform better
 * than the default Lucene equivalent which are optimised for fuzzy fulltext search.
 */
class CypherStringQueryFactory {
    static Query stringPrefix(String prefix) {
        Term term = new Term(ValueEncoding.String.key(0), prefix);
        return new PrefixMultiTermsQuery(term);
    }

    static Query stringContains(String substring) {
        Term term = new Term(ValueEncoding.String.key(0), substring);
        return new ContainsMultiTermsQuery(term);
    }

    static Query stringSuffix(String suffix) {
        Term term = new Term(ValueEncoding.String.key(0), suffix);
        return new SuffixMultiTermsQuery(term);
    }

    /**
     * The standard Lucene query for this is {@link org.apache.lucene.search.PrefixQuery}.
     * It uses an automaton which is very expensive to construct and not so cheap to use either.
     * It turns out that for the type of indexes we use this for,
     * a simple term seek and iterating over the terms is just much faster.
     */
    private static class PrefixMultiTermsQuery extends MultiTermQuery {
        private final Term term;

        PrefixMultiTermsQuery(Term term) {
            super(term.field(), CONSTANT_SCORE_REWRITE);
            this.term = term;
        }

        @Override
        protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
            return term.bytes().length == 0 ? terms.iterator() : new PrefixTermsEnum(terms.iterator(), term.bytes());
        }

        @Override
        public String toString(String field) {
            return getClass().getSimpleName() + ", term:" + term + ", field:" + field;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(term.field())) {
                visitor.consumeTerms(this, term);
            }
        }

        private static class PrefixTermsEnum extends FilteredTermsEnum {
            private final BytesRef prefix;

            PrefixTermsEnum(TermsEnum termEnum, BytesRef prefix) {
                super(termEnum);
                this.prefix = prefix;
                setInitialSeekTerm(this.prefix);
            }

            @Override
            protected AcceptStatus accept(BytesRef term) {
                return StringHelper.startsWith(term, prefix) ? AcceptStatus.YES : AcceptStatus.END;
            }
        }
    }

    /**
     * The standard Lucene query for this is {@link org.apache.lucene.search.WildcardQuery}.
     * It uses an automaton which is very expensive to construct and not so cheap to use either.
     * For wildcard queries that start with a wildcard, there is nothing smarter that can be done
     * than scanning all the terms. There is no need to construct and use a smart automaton
     * for such a brute force operation.
     * <p>
     * This is an important extract from javadoc of {@link org.apache.lucene.search.WildcardQuery}:
     * 'Note this query can be slow, as it needs to iterate over many terms.
     * In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with the wildcard *'
     */
    private static class ContainsMultiTermsQuery extends MultiTermQuery {
        private final Term term;

        ContainsMultiTermsQuery(Term term) {
            super(term.field(), CONSTANT_SCORE_REWRITE);
            this.term = term;
        }

        @Override
        protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
            return term.bytes().length == 0 ? terms.iterator() : new ContainsTermsEnum(terms.iterator(), term.bytes());
        }

        @Override
        public String toString(String field) {
            return getClass().getSimpleName() + ", term:" + term + ", field:" + field;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(term.field())) {
                visitor.consumeTerms(this, term);
            }
        }

        private static class ContainsTermsEnum extends FilteredTermsEnum {
            private final BytesRef substring;

            ContainsTermsEnum(TermsEnum termsEnum, BytesRef substring) {
                super(termsEnum, false);
                this.substring = substring;
            }

            @Override
            protected AcceptStatus accept(BytesRef term) {
                // the following code would blow up for substring.length == 0,
                // but we don't create this class for such cases

                if (substring.length > term.length) {
                    return AcceptStatus.NO;
                }

                final byte first = substring.bytes[substring.offset];
                final int max = term.offset + term.length - substring.length;
                for (int pos = term.offset; pos <= max; pos++) {
                    // find first byte
                    if (term.bytes[pos] != first) {
                        while (++pos <= max && term.bytes[pos] != first) {
                            // do nothing
                        }
                    }

                    // Now we have the first byte match, look at the rest
                    if (pos <= max) {
                        int i = pos + 1;
                        final int end = pos + substring.length;
                        for (int j = substring.offset + 1; i < end && term.bytes[i] == substring.bytes[j]; j++, i++) {
                            // do nothing
                        }

                        if (i == end) {
                            return AcceptStatus.YES;
                        }
                    }
                }
                return AcceptStatus.NO;
            }
        }
    }

    /**
     * This is a very similar problem as the one solved by {@link ContainsMultiTermsQuery}
     * and the explanation for that class holds here, too.
     */
    private static class SuffixMultiTermsQuery extends MultiTermQuery {
        private final Term term;

        SuffixMultiTermsQuery(Term term) {
            super(term.field(), CONSTANT_SCORE_REWRITE);
            this.term = term;
        }

        @Override
        protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
            return term.bytes().length == 0 ? terms.iterator() : new SuffixTermsEnum(terms.iterator(), term.bytes());
        }

        @Override
        public String toString(String field) {
            return getClass().getSimpleName() + ", term:" + term + ", field:" + field;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(term.field())) {
                visitor.consumeTerms(this, term);
            }
        }

        private static class SuffixTermsEnum extends FilteredTermsEnum {
            private final BytesRef suffix;

            SuffixTermsEnum(TermsEnum termsEnum, BytesRef suffix) {
                super(termsEnum, false);
                this.suffix = suffix;
            }

            @Override
            protected AcceptStatus accept(BytesRef term) {
                return StringHelper.endsWith(term, suffix) ? AcceptStatus.YES : AcceptStatus.NO;
            }
        }
    }
}
