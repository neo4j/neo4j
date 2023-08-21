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
package org.neo4j.kernel.api.impl.index;

import static java.lang.Integer.max;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.document.Document;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.IOUtils;

public class LuceneAllDocumentsReader implements BoundedIterable<Document> {
    private final List<LucenePartitionAllDocumentsReader> partitionReaders;

    public LuceneAllDocumentsReader(List<LucenePartitionAllDocumentsReader> partitionReaders) {
        this.partitionReaders = partitionReaders;
    }

    @Override
    public long maxCount() {
        return partitionReaders.stream()
                .mapToLong(LucenePartitionAllDocumentsReader::maxCount)
                .sum();
    }

    @Override
    public Iterator<Document> iterator() {
        Iterator<Iterator<Document>> iterators = partitionReaders.stream()
                .map(LucenePartitionAllDocumentsReader::iterator)
                .collect(toList())
                .iterator();

        return Iterators.concat(iterators);
    }

    /**
     * Partitions all documents in this index into (at most) {@code numPartitions} partitions, each reading its own document ID range.
     *
     * @param numPartitions number of desired partitions to return.
     * @return a list of document iterators, each reading its own document ID range.
     */
    public List<Iterator<Document>> partition(int numPartitions) {
        int partitionsPerIndexPartition = max(1, numPartitions / partitionReaders.size());
        List<Iterator<Document>> result = new ArrayList<>();
        for (LucenePartitionAllDocumentsReader partitionReader : partitionReaders) {
            int indexPartitionMaxCount = toIntExact(partitionReader.maxCount());
            int roughCountPerIndexPartition = indexPartitionMaxCount / partitionsPerIndexPartition;
            for (int i = 0; i < partitionsPerIndexPartition; i++) {
                int from = i * roughCountPerIndexPartition;
                int to = i == partitionsPerIndexPartition - 1
                        ? indexPartitionMaxCount
                        : from + roughCountPerIndexPartition;
                result.add(partitionReader.iterator(from, to));
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(partitionReaders);
    }
}
