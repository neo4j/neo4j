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
package org.neo4j.kernel.api.impl.schema.fulltext;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.schema.writer.LucenePartitionIndexWriter;

class TransactionStateLuceneIndexWriter implements LucenePartitionIndexWriter, Closeable {
    private final Config config;
    private final Analyzer analyzer;
    private LuceneIndexWriter writer;
    private final LuceneDirectory directory;

    TransactionStateLuceneIndexWriter(LuceneContext luceneContext, Config config, Analyzer analyzer) {
        this.config = config;
        this.analyzer = analyzer;
        this.directory = luceneContext.directoryFactory().inMemoryDirectory();
    }

    @Override
    public LuceneDocumentsFactory documentsFactory() {
        return directory.getLuceneContext().documentsFactory();
    }

    @Override
    public void addDocument(LuceneDocument document) throws IOException {
        writer.addDocument(document);
    }

    @Override
    public void addDocuments(int numDocs, Iterable<LuceneDocument> document) throws IOException {
        writer.addDocuments(document);
    }

    @Override
    public void updateDocument(String idField, long id, LuceneDocument document) throws IOException {
        writer.updateDocument(idField, id, document);
    }

    @Override
    public void deleteDocuments(String idField, long id) throws IOException {
        writer.deleteDocuments(idField, id);
    }

    @Override
    public void addDirectory(int count, LuceneDirectory directory) throws IOException {
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
        writer = directory.newWriter(new IndexWriterConfigBuilder(IndexWriterConfigMode.TRANSACTION_STATE, config)
                .withAnalyzer(analyzer)
                .build());
    }

    SearcherReference getNearRealTimeSearcher() throws IOException {
        LuceneDirectoryReader directoryReader = writer.directoryReader();
        return new DirectSearcherReference(directoryReader.newDirectSearcher(), directoryReader);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(writer, directory);
    }
}
