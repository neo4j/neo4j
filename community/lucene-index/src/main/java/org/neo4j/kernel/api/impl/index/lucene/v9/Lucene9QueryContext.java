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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import static org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory.TRIGRAM_VALUE_KEY;
import static org.neo4j.kernel.api.impl.index.lucene.v9.Lucene9Utils.loadAnalyzer;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.neo4j.exceptions.InternalException;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryParseException;
import org.neo4j.kernel.api.impl.index.lucene.v9.Lucene9TrigramTokenStream.CodePointBuffer;
import org.neo4j.kernel.api.impl.schema.TextDocumentStructure;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.shaded.lucene9.analysis.CharacterUtils;
import org.neo4j.shaded.lucene9.index.FilteredTermsEnum;
import org.neo4j.shaded.lucene9.index.Term;
import org.neo4j.shaded.lucene9.index.Terms;
import org.neo4j.shaded.lucene9.index.TermsEnum;
import org.neo4j.shaded.lucene9.queryparser.classic.MultiFieldQueryParser;
import org.neo4j.shaded.lucene9.queryparser.classic.ParseException;
import org.neo4j.shaded.lucene9.queryparser.classic.QueryParserBase;
import org.neo4j.shaded.lucene9.search.BooleanClause;
import org.neo4j.shaded.lucene9.search.BooleanQuery;
import org.neo4j.shaded.lucene9.search.ConstantScoreQuery;
import org.neo4j.shaded.lucene9.search.KnnFloatVectorQuery;
import org.neo4j.shaded.lucene9.search.MatchAllDocsQuery;
import org.neo4j.shaded.lucene9.search.MultiTermQuery;
import org.neo4j.shaded.lucene9.search.Query;
import org.neo4j.shaded.lucene9.search.QueryVisitor;
import org.neo4j.shaded.lucene9.search.TermQuery;
import org.neo4j.shaded.lucene9.search.WildcardQuery;
import org.neo4j.shaded.lucene9.util.AttributeSource;
import org.neo4j.shaded.lucene9.util.BytesRef;
import org.neo4j.shaded.lucene9.util.StringHelper;
import org.neo4j.values.storable.Value;

public class Lucene9QueryContext implements LuceneQueryContext {
    private BooleanQuery.Builder booleanBuilder;
    private Query singleQuery;

    static Lucene9QueryContext wrap(Query query) {
        Lucene9QueryContext queryContext = new Lucene9QueryContext();
        queryContext.assignSingle(query);
        return queryContext;
    }

    @Override
    public Lucene9QueryContext addMustTerm(String field, String text) {
        ensureBooleanBuilder();
        booleanBuilder.add(new TermQuery(new Term(field, text)), BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene9QueryContext addMustNotHaveField(String field) {
        ensureBooleanBuilder();
        booleanBuilder.add(
                new ConstantScoreQuery(new WildcardQuery(new Term(field, "*"))), BooleanClause.Occur.MUST_NOT);
        return this;
    }

    @Override
    public Lucene9QueryContext addShouldQueryText(String query, String[] fields, Analyzer analyzer)
            throws LuceneQueryParseException {
        try {
            ensureBooleanBuilder();
            booleanBuilder.add(parseFulltextQuery(query, fields, analyzer), BooleanClause.Occur.SHOULD);
            return this;
        } catch (ParseException e) {
            throw new LuceneQueryParseException(e);
        }
    }

    @Override
    public Lucene9QueryContext exactTerm(String entityIdKey, long entityId) {
        assignSingle(new TermQuery(new Term(entityIdKey, Long.toString(entityId))));
        return this;
    }

    @Override
    public Lucene9QueryContext addExactTrigram(String value) {
        ensureBooleanBuilder();
        booleanBuilder.add(trigramSearchQuery(value), BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene9QueryContext addConstantMustTerm(String field, String text) {
        ensureBooleanBuilder();
        ConstantScoreQuery termQuery = new ConstantScoreQuery(new TermQuery(new Term(field, text)));
        booleanBuilder.add(termQuery, BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene9QueryContext addMustSeek(Value... propertyValues) {
        ensureBooleanBuilder();
        Lucene9QueryContext queryContext = new Lucene9QueryContext();
        TextDocumentStructure.seekStrings(propertyValues, queryContext);
        booleanBuilder.add(queryContext.build(), BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene9QueryContext matchAll() {
        assignSingle(new MatchAllDocsQuery());
        return this;
    }

    @Override
    public Lucene9QueryContext stringPrefix(String prefix) {
        Term term = new Term(LuceneDocumentsFactory.textValueKey(0), prefix);
        assignSingle(new PrefixMultiTermsQuery(term));
        return this;
    }

    @Override
    public Lucene9QueryContext stringContains(String substring) {
        Term term = new Term(LuceneDocumentsFactory.textValueKey(0), substring);
        assignSingle(new ContainsMultiTermsQuery(term));
        return this;
    }

    @Override
    public Lucene9QueryContext stringSuffix(String suffix) {
        Term term = new Term(LuceneDocumentsFactory.textValueKey(0), suffix);
        assignSingle(new SuffixMultiTermsQuery(term));
        return this;
    }

    @Override
    public Lucene9QueryContext trigramSearch(String searchString) {
        assignSingle(trigramSearchQuery(searchString));
        return this;
    }

    @Override
    public Lucene9QueryContext approximateNearestNeighbors(
            VectorDocumentStructure documentStructure, float[] query, int k, int efSearch, boolean rescore) {
        if (rescore) {
            throw InternalException.internalError(
                    getClass().getSimpleName(), "Rescoring is not supported in this index");
        }
        assignSingle(new KnnFloatVectorQuery(documentStructure.vectorValueKeyFor(query.length), query, efSearch));
        return this;
    }

    @Override
    public Lucene9QueryContext approximateNearestNeighbors(
            VectorDocumentStructure documentStructure,
            float[] query,
            int k,
            int efSearch,
            boolean rescore,
            EntityFilterPredicate entityFilter,
            PropertyIndexQuery... filterQueries) {
        if (entityFilter != EntityFilterPredicate.MatchAll.INSTANCE || filterQueries.length != 0) {
            throw InternalException.internalError(
                    getClass().getSimpleName(), "Single stage filter is not supported in this index");
        }
        return approximateNearestNeighbors(documentStructure, query, k, efSearch, rescore);
    }

    public Query build() {
        if (singleQuery != null) {
            return singleQuery;
        }
        return booleanBuilder.build();
    }

    private void ensureBooleanBuilder() {
        if (booleanBuilder == null) {
            if (singleQuery != null) {
                throw new IllegalStateException("Builder already assigned to an absolute query");
            }
            booleanBuilder = new BooleanQuery.Builder();
        }
    }

    private void assignSingle(Query single) {
        if (booleanBuilder != null) {
            throw new IllegalStateException("Cannot combine boolean with absolute queries");
        }
        if (singleQuery != null) {
            throw new IllegalStateException("Can only have one absolute query");
        }
        singleQuery = single;
    }

    private static Query parseFulltextQuery(String query, String[] propertyNames, Analyzer analyzer)
            throws ParseException {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser(propertyNames, loadAnalyzer(analyzer));
        multiFieldQueryParser.setAllowLeadingWildcard(true);
        return multiFieldQueryParser.parse(query);
    }

    private static Query trigramSearchQuery(String searchString) {
        if (searchString.isEmpty()) {
            return new MatchAllDocsQuery();
        }

        CodePointBuffer codePointBuffer = Lucene9TrigramTokenStream.getCodePoints(searchString);

        if (codePointBuffer.codePointCount() < 3) {
            String searchTerm = QueryParserBase.escape(searchString);
            Term term = new Term(TRIGRAM_VALUE_KEY, "*" + searchTerm + "*");
            return new WildcardQuery(term);
        }

        Lucene9QueryContext builder = new Lucene9QueryContext();
        // Don't generate more clauses than what is allowed by IndexSearcher.
        // Default value for IndexSearcher.getMaxClauseCount() is 1024 which is assumed to be enough to not generate too
        // many false positives. And those false positives will be filtered out later as usual.
        for (int i = 0; i < codePointBuffer.codePointCount() - 2 && i < LuceneIndexSearcher.getMaxClauseCount(); i++) {
            String term = getNgram(codePointBuffer, i, 3);

            builder.addConstantMustTerm(TRIGRAM_VALUE_KEY, term);
        }
        return builder.build();
    }

    private static String getNgram(Lucene9TrigramTokenStream.CodePointBuffer codePointBuffer, int ngramIndex, int n) {
        char[] termCharBuffer = new char[2 * n];
        int length = CharacterUtils.toChars(codePointBuffer.codePoints(), ngramIndex, n, termCharBuffer, 0);
        return new String(termCharBuffer, 0, length);
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
     * The standard Lucene query for this is {@link WildcardQuery}.
     * It uses an automaton which is very expensive to construct and not so cheap to use either.
     * For wildcard queries that start with a wildcard, there is nothing smarter that can be done
     * than scanning all the terms. There is no need to construct and use a smart automaton
     * for such a brute force operation.
     * <p>
     * This is an important extract from javadoc of {@link WildcardQuery}:
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

                byte first = substring.bytes[substring.offset];
                int max = term.offset + term.length - substring.length;
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
                        int end = pos + substring.length;
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
