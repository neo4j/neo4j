/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleFulltextReader}s for individual partitions.
 *
 * @see SimpleFulltextReader
 */
class PartitionedFulltextReader implements ReadOnlyFulltext
{

    private final List<ReadOnlyFulltext> indexReaders;

    PartitionedFulltextReader( List<PartitionSearcher> partitionSearchers, String[] properties, Analyzer analyzer )
    {
        this( partitionSearchers.stream().map( partitionSearcher -> new SimpleFulltextReader( partitionSearcher, properties, analyzer ) ).collect(
                Collectors.toList() ) );
    }

    private PartitionedFulltextReader( List<ReadOnlyFulltext> readers )
    {
        this.indexReaders = readers;
    }

    @Override
    public ScoreEntityIterator query( Collection<String> terms, boolean matchAll )
    {
        return partitionedOperation( reader -> innerQuery( reader, matchAll, terms ) );
    }

    @Override
    public ScoreEntityIterator fuzzyQuery( Collection<String> terms, boolean matchAll )
    {
        return partitionedOperation( reader -> innerFuzzyQuery( reader, matchAll, terms ) );
    }

    private ScoreEntityIterator innerQuery( ReadOnlyFulltext reader, boolean matchAll, Collection<String> query )
    {
        return reader.query( query, matchAll );
    }

    private ScoreEntityIterator innerFuzzyQuery( ReadOnlyFulltext reader, boolean matchAll, Collection<String> query )
    {
        return reader.fuzzyQuery( query, matchAll );
    }

    @Override
    public void close()
    {
        try
        {
            IOUtils.closeAll( indexReaders );
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    @Override
    public FulltextIndexConfiguration getConfigurationDocument() throws IOException
    {
        for ( ReadOnlyFulltext indexReader : indexReaders )
        {
            FulltextIndexConfiguration config = indexReader.getConfigurationDocument();
            if ( config != null )
            {
                return config;
            }
        }
        return null;
    }

    private ScoreEntityIterator partitionedOperation( Function<ReadOnlyFulltext,ScoreEntityIterator> readerFunction )
    {
        return ScoreEntityIterator.concat( indexReaders.parallelStream().map( readerFunction ).collect( Collectors.toList() ) );
    }
}
