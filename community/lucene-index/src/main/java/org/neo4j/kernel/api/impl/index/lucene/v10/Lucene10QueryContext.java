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

import static org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory.TRIGRAM_VALUE_KEY;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.FullPrecisionFloatVectorSimilarityValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RescoreTopNQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryParseException;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10TrigramTokenStream.CodePointBuffer;
import org.neo4j.kernel.api.impl.schema.TextDocumentStructure;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

public class Lucene10QueryContext implements LuceneQueryContext {
    private BooleanQuery.Builder booleanBuilder;
    private Query singleQuery;

    static Lucene10QueryContext wrap(Query query) {
        Lucene10QueryContext queryContext = new Lucene10QueryContext();
        queryContext.assignSingle(query);
        return queryContext;
    }

    @Override
    public Lucene10QueryContext addMustTerm(String field, String text) {
        ensureBooleanBuilder();
        booleanBuilder.add(new TermQuery(new Term(field, text)), BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene10QueryContext addMustNotHaveField(String field) {
        ensureBooleanBuilder();
        booleanBuilder.add(
                new ConstantScoreQuery(new WildcardQuery(new Term(field, "*"))), BooleanClause.Occur.MUST_NOT);
        return this;
    }

    @Override
    public Lucene10QueryContext addShouldQueryText(String query, String[] fields, Analyzer analyzer)
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
    public Lucene10QueryContext exactTerm(String entityIdKey, long entityId) {
        assignSingle(new TermQuery(new Term(entityIdKey, Long.toString(entityId))));
        return this;
    }

    @Override
    public Lucene10QueryContext addExactTrigram(String value) {
        ensureBooleanBuilder();
        booleanBuilder.add(trigramSearchQuery(value), BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene10QueryContext addConstantMustTerm(String field, String text) {
        ensureBooleanBuilder();
        ConstantScoreQuery termQuery = new ConstantScoreQuery(new TermQuery(new Term(field, text)));
        booleanBuilder.add(termQuery, BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene10QueryContext addMustSeek(Value... propertyValues) {
        ensureBooleanBuilder();
        Lucene10QueryContext queryContext = new Lucene10QueryContext();
        TextDocumentStructure.seekStrings(propertyValues, queryContext);
        booleanBuilder.add(queryContext.build(), BooleanClause.Occur.MUST);
        return this;
    }

    @Override
    public Lucene10QueryContext matchAll() {
        assignSingle(MatchAllDocsQuery.INSTANCE);
        return this;
    }

    @Override
    public Lucene10QueryContext stringPrefix(String prefix) {
        Term term = new Term(LuceneDocumentsFactory.textValueKey(0), prefix);
        assignSingle(new PrefixMultiTermsQuery(term));
        return this;
    }

    @Override
    public Lucene10QueryContext stringContains(String substring) {
        Term term = new Term(LuceneDocumentsFactory.textValueKey(0), substring);
        assignSingle(new ContainsMultiTermsQuery(term));
        return this;
    }

    @Override
    public Lucene10QueryContext stringSuffix(String suffix) {
        Term term = new Term(LuceneDocumentsFactory.textValueKey(0), suffix);
        assignSingle(new SuffixMultiTermsQuery(term));
        return this;
    }

    @Override
    public Lucene10QueryContext trigramSearch(String searchString) {
        assignSingle(trigramSearchQuery(searchString));
        return this;
    }

    @Override
    public Lucene10QueryContext approximateNearestNeighbors(
            VectorDocumentStructure documentStructure, float[] query, int k, int efSearch, boolean rescore) {
        assignSingle(annQuery(documentStructure, query, k, efSearch, rescore, null));
        return this;
    }

    @Override
    public Lucene10QueryContext approximateNearestNeighbors(
            VectorDocumentStructure documentStructure,
            float[] query,
            int k,
            int efSearch,
            boolean rescore,
            EntityFilterPredicate entityFilter,
            PropertyIndexQuery... filterQueries) {
        Query filters = Lucene10FilterQueryBuilder.build(documentStructure, entityFilter, filterQueries);
        assignSingle(annQuery(documentStructure, query, k, efSearch, rescore, filters));
        return this;
    }

    private static Query annQuery(
            VectorDocumentStructure documentStructure,
            float[] query,
            int k,
            int efSearch,
            boolean rescore,
            Query filter) {
        assert efSearch >= k : "efSearch must be >= k";
        String field = documentStructure.vectorValueKeyFor(query.length);
        Query vectorQuery = new KnnFloatVectorQuery(field, query, efSearch, filter);
        return rescore ? new RescoreOrEmptyQuery(vectorQuery, query, field, k) : vectorQuery;
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
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser(propertyNames, analyzer);
        multiFieldQueryParser.setAllowLeadingWildcard(true);
        return multiFieldQueryParser.parse(query);
    }

    private static Query trigramSearchQuery(String searchString) {
        if (searchString.isEmpty()) {
            return MatchAllDocsQuery.INSTANCE;
        }

        CodePointBuffer codePointBuffer = Lucene10TrigramTokenStream.getCodePoints(searchString);

        if (codePointBuffer.codePointCount() < 3) {
            String searchTerm = QueryParserBase.escape(searchString);
            Term term = new Term(TRIGRAM_VALUE_KEY, "*" + searchTerm + "*");
            return new WildcardQuery(term);
        }

        Lucene10QueryContext builder = new Lucene10QueryContext();
        // Don't generate more clauses than what is allowed by IndexSearcher.
        // Default value for IndexSearcher.getMaxClauseCount() is 1024 which is assumed to be enough to not generate too
        // many false positives. And those false positives will be filtered out later as usual.
        for (int i = 0; i < codePointBuffer.codePointCount() - 2 && i < LuceneIndexSearcher.getMaxClauseCount(); i++) {
            String term = getNgram(codePointBuffer, i, 3);

            builder.addConstantMustTerm(TRIGRAM_VALUE_KEY, term);
        }
        return builder.build();
    }

    private static String getNgram(Lucene10TrigramTokenStream.CodePointBuffer codePointBuffer, int ngramIndex, int n) {
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

    private static class RescoreOrEmptyQuery extends Query {
        private final Query delegate;

        RescoreOrEmptyQuery(Query vectorQuery, float[] query, String field, int n) {
            // cannot use RescoreTopNQuery::createFullPrecisionRescorerQuery due to null VectorSimilarityFunction
            this.delegate =
                    new RescoreTopNQuery(new NonEmptyQuery(vectorQuery), new VectorValuesSource(query, field), n);
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            return indexSearcher.getIndexReader().leaves().isEmpty()
                    ? MatchNoDocsQuery.INSTANCE
                    : delegate.rewrite(indexSearcher);
        }

        @Override
        public String toString(String field) {
            return delegate.toString(field);
        }

        @Override
        public void visit(QueryVisitor visitor) {
            delegate.visit(visitor);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof RescoreOrEmptyQuery that && this.delegate.equals(that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), delegate);
        }
    }

    private static class VectorValuesSource extends DoubleValuesSource {
        private final DoubleValuesSource delegate;
        private final float[] queryVector;
        private final String fieldName;

        VectorValuesSource(float[] vector, String fieldName) {
            this.delegate = new FullPrecisionFloatVectorSimilarityValuesSource(vector, fieldName);
            this.queryVector = vector;
            this.fieldName = fieldName;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + '('
                    + "fieldName=" + fieldName
                    + " queryVector=" + Arrays.toString(queryVector)
                    + ')';
        }

        @Override
        public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
            return delegate.getValues(ctx, scores);
        }

        @Override
        public boolean needsScores() {
            return delegate.needsScores();
        }

        @Override
        public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
            return delegate.rewrite(reader);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof VectorValuesSource that && this.delegate.equals(that.delegate);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return delegate.isCacheable(ctx);
        }
    }

    // needs to be in (LuceneIndexWriter.MAX_DOCS, DocIdSetIterator.NO_MORE_DOCS)
    // which is a range of docs that can't exist
    static final int FAKE_DOC = DocIdSetIterator.NO_MORE_DOCS - 1;

    static {
        Preconditions.requireBetween(FAKE_DOC, LuceneIndexWriter.MAX_DOCS + 1, DocIdSetIterator.NO_MORE_DOCS);
    }

    private static final class NonEmptyQuery extends Query {
        private final Query delegate;

        NonEmptyQuery(Query delegate) {
            this.delegate = delegate;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new NonEmptyWeight(searcher.createWeight(delegate, scoreMode, boost));
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            Query inner = indexSearcher.rewrite(delegate);
            return inner != delegate ? new NonEmptyQuery(inner) : this;
        }

        @Override
        public String toString(String field) {
            return delegate.toString(field);
        }

        @Override
        public void visit(QueryVisitor visitor) {
            delegate.visit(visitor);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NonEmptyQuery that && this.delegate.equals(that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), delegate);
        }

        private static final class NonEmptyWeight extends FilterWeight {

            NonEmptyWeight(Weight delegate) {
                super(delegate);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                ScorerSupplier inner = in.scorerSupplier(context);
                return inner == null
                        ? new DefaultScorerSupplier(new FakeEntryScorer(context.docBase))
                        : new NonEmptyScorerSupplier(inner, context.docBase);
            }
        }

        private static final class NonEmptyScorerSupplier extends ScorerSupplier {
            private final ScorerSupplier delegate;
            private final int docIdBase;

            NonEmptyScorerSupplier(ScorerSupplier delegate, int docIdBase) {
                this.delegate = delegate;
                this.docIdBase = docIdBase;
            }

            @Override
            public Scorer get(long leadCost) throws IOException {
                return new NonEmptyScorer(delegate.get(leadCost), docIdBase);
            }

            @Override
            public long cost() {
                return delegate.cost();
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                return delegate.bulkScorer();
            }

            @Override
            public void setTopLevelScoringClause() throws IOException {
                delegate.setTopLevelScoringClause();
            }
        }

        private static final class NonEmptyScorer extends Scorer {
            private final DocIdSetIterator iterator;

            NonEmptyScorer(Scorer delegate, int docIdBase) {
                iterator = new NonEmptyDocIdSetIterator(delegate.iterator(), docIdBase);
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }

            @Override
            public float getMaxScore(int upTo) {
                return 0.0f;
            }

            @Override
            public float score() {
                return 0.0f;
            }
        }

        private static final class FakeEntryScorer extends Scorer {
            private final DocIdSetIterator iterator;

            FakeEntryScorer(int docIdBase) {
                int fake = FAKE_DOC - docIdBase;
                iterator = DocIdSetIterator.range(fake, fake + 1);
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }

            @Override
            public float getMaxScore(int upTo) {
                return 0.0f;
            }

            @Override
            public float score() {
                return 0.0f;
            }
        }

        private static final class NonEmptyDocIdSetIterator extends FilterDocIdSetIterator {
            private static final int NOT_STARTED = -1;

            private final DocIdSetIterator delegate;
            private final int docIdBase;

            private int doc = NOT_STARTED;

            NonEmptyDocIdSetIterator(DocIdSetIterator delegate, int docIdBase) {
                super(delegate);
                this.delegate = delegate;
                this.docIdBase = docIdBase;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                // guard against calling inner again after it's been exhausted
                if (this.doc == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }

                int doc = delegate.nextDoc();
                // inner returns a value
                if (doc != NO_MORE_DOCS) {
                    return this.doc = doc;
                }

                // we had set at least one doc before, so it's already non-empty
                if (this.doc != NOT_STARTED) {
                    return this.doc = NO_MORE_DOCS;
                }

                // make up fake doc
                this.doc = NO_MORE_DOCS;
                return FAKE_DOC - docIdBase;
            }
        }
    }
}
