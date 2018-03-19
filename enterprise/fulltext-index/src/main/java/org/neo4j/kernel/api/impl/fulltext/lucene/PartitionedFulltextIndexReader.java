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
package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleFulltextIndexReader}s for individual partitions.
 *
 * @see SimpleFulltextIndexReader
 */
class PartitionedFulltextIndexReader extends FulltextIndexReader
{

    private final List<FulltextIndexReader> indexReaders;

    PartitionedFulltextIndexReader( List<PartitionSearcher> partitionSearchers, String[] properties, Analyzer analyzer )
    {
        this( partitionSearchers.stream().map( partitionSearcher -> new SimpleFulltextIndexReader( partitionSearcher, properties, analyzer ) ).collect(
                Collectors.toList() ) );
    }

    private PartitionedFulltextIndexReader( List<FulltextIndexReader> readers )
    {
        this.indexReaders = readers;
    }

    @Override
    public ScoreEntityIterator query( String query ) throws ParseException
    {
        return partitionedQuery( query );
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

    private ScoreEntityIterator partitionedQuery( String query ) throws ParseException
    {
        List<ScoreEntityIterator> results = new ArrayList<>();
        for ( FulltextIndexReader indexReader : indexReaders )
        {
            results.add( indexReader.query( query ) );
        }
        return ScoreEntityIterator.concat( results );
    }
}
