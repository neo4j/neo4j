/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

public interface LabelScanStorageStrategy
{
    PrimitiveLongIterator nodesWithLabel( IndexSearcher searcher, int labelId );

    AllEntriesLabelScanReader newNodeLabelReader( SearcherManager searcher );

    Iterator<Long> labelsForNode( IndexSearcher searcher, long nodeId );

    LabelScanWriter acquireWriter( StorageService storage, Lock heldLock );

    interface StorageService
    {
        void updateDocument( Term documentTerm, Document document ) throws IOException, IndexCapacityExceededException;

        void deleteDocuments( Term documentTerm ) throws IOException;

        IndexSearcher acquireSearcher();

        void releaseSearcher( IndexSearcher searcher ) throws IOException;

        void refreshSearcher() throws IOException;
    }
}
