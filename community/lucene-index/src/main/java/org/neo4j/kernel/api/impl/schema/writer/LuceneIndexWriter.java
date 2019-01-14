/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema.writer;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import java.io.IOException;

/**
 * A thin wrapper around {@link org.apache.lucene.index.IndexWriter} that exposes only some part of it's
 * functionality that it really needed and hides a fact that index is partitioned.
 */
public interface LuceneIndexWriter
{
    void addDocument( Document document ) throws IOException;

    void addDocuments( int numDocs, Iterable<Document> document ) throws IOException;

    void updateDocument( Term term, Document document ) throws IOException;

    void deleteDocuments( Term term ) throws IOException;

    void deleteDocuments( Query query ) throws IOException;
}
