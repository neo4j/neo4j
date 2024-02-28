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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigModes.FulltextModes;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;

class TransactionStateLuceneIndexWriter implements LuceneIndexWriter, Closeable {
    private final Config config;
    private final Analyzer analyzer;
    private final IndexConfig indexConfig;
    private IndexWriter writer;
    private final Directory directory;

    TransactionStateLuceneIndexWriter(Config config, Analyzer analyzer, IndexConfig indexConfig) {
        this.config = config;
        this.analyzer = analyzer;
        this.indexConfig = indexConfig;
        directory = new ByteBuffersDirectory();
    }

    @Override
    public void addDocument(Document document) throws IOException {
        writer.addDocument(document);
    }

    @Override
    public void addDocuments(int numDocs, Iterable<Document> document) throws IOException {
        writer.addDocuments(document);
    }

    @Override
    public void updateDocument(Term term, Document document) throws IOException {
        writer.updateDocument(term, document);
    }

    @Override
    public void deleteDocuments(Term term) throws IOException {
        writer.deleteDocuments(term);
    }

    @Override
    public void deleteDocuments(Query query) throws IOException {
        writer.deleteDocuments(query);
    }

    @Override
    public void addDirectory(int count, Directory directory) throws IOException {
        writer.addIndexes(directory);
    }

    void resetWriterState() throws IOException {
        if (writer != null) {
            // Note that 'rollback' closes the writer.
            writer.rollback();
        }
        openWriter();
    }

    private void openWriter() throws IOException {
        writer = new IndexWriter(
                directory,
                new IndexWriterConfigBuilder(FulltextModes.TRANSACTION_STATE, config)
                        .withAnalyzer(analyzer)
                        .build());
    }

    SearcherReference getNearRealTimeSearcher() throws IOException {
        DirectoryReader directoryReader = DirectoryReader.open(writer);
        Neo4jIndexSearcher searcher = new Neo4jIndexSearcher(directoryReader);
        return new DirectSearcherReference(searcher, directoryReader);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(writer, directory);
    }
}
