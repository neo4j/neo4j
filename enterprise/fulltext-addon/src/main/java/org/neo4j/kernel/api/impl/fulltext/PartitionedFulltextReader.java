/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
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
    private final Consumer<ReadOnlyFulltext> closeCallback;

    PartitionedFulltextReader( List<PartitionSearcher> partitionSearchers, String[] properties, Analyzer analyzer, Consumer<ReadOnlyFulltext> closeCallback )
    {
        this.indexReaders =
                partitionSearchers.stream().map( partitionSearcher -> new SimpleFulltextReader( partitionSearcher, properties, analyzer,
                        PartitionedFulltextReader::nullCallback ) ).collect( Collectors.toList() );
        this.closeCallback = closeCallback;
    }

    private static void nullCallback( ReadOnlyFulltext reader )
    {
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
        closeCallback.accept( this );
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
