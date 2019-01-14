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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.IOUtils;

import static java.util.stream.Collectors.toList;

public class LuceneAllDocumentsReader implements BoundedIterable<Document>
{
    private final List<LucenePartitionAllDocumentsReader> partitionReaders;

    public LuceneAllDocumentsReader( List<LucenePartitionAllDocumentsReader> partitionReaders )
    {
        this.partitionReaders = partitionReaders;
    }

    @Override
    public long maxCount()
    {
        return partitionReaders.stream().mapToLong( LucenePartitionAllDocumentsReader::maxCount ).sum();
    }

    @Override
    public Iterator<Document> iterator()
    {
        Iterator<Iterator<Document>> iterators = partitionReaders.stream()
                .map( LucenePartitionAllDocumentsReader::iterator )
                .collect( toList() )
                .iterator();

        return Iterators.concat( iterators );
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( partitionReaders );
    }
}
