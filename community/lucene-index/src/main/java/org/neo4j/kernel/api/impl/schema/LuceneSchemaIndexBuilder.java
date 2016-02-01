/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Helper builder class to simplify construction and instantiation of lucene schema indexes.
 * Most of the values already have most useful default value, that still can be overridden by corresponding
 * builder methods.
 *
 * @see LuceneSchemaIndex
 * @see AbstractLuceneIndexBuilder
 */
public class LuceneSchemaIndexBuilder extends AbstractLuceneIndexBuilder<LuceneSchemaIndexBuilder>
{
    private IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
    private IndexConfiguration indexConfig = IndexConfiguration.NON_UNIQUE;
    private Supplier<IndexWriterConfig> writerConfigSupplier = IndexWriterConfigs::standard;

    private LuceneSchemaIndexBuilder()
    {
    }

    /**
     * Create new lucene schema index builder.
     *
     * @return new LuceneSchemaIndexBuilder
     */
    public static LuceneSchemaIndexBuilder create()
    {
        return new LuceneSchemaIndexBuilder();
    }

    /**
     * Specify lucene schema index sampling config
     *
     * @param samplingConfig sampling config
     * @return index builder
     */
    public LuceneSchemaIndexBuilder withSamplingConfig( IndexSamplingConfig samplingConfig )
    {
        this.samplingConfig = samplingConfig;
        return this;
    }

    /**
     * Specify lucene schema index sampling buffer size
     *
     * @param size sampling buffer size
     * @return index builder
     */
    public LuceneSchemaIndexBuilder withSamplingBufferSize( int size )
    {
        Map<String,String> params = stringMap( GraphDatabaseSettings.index_sampling_buffer_size.name(), size + "" );
        Config config = new Config( params );
        this.samplingConfig = new IndexSamplingConfig( config );
        return this;
    }

    /**
     * Specify lucene schema index config
     *
     * @param indexConfig index config
     * @return index builder
     */
    public LuceneSchemaIndexBuilder withIndexConfig( IndexConfiguration indexConfig )
    {
        this.indexConfig = indexConfig;
        return this;
    }

    /**
     * Specify {@link Supplier} of lucene {@link IndexWriterConfig} to create {@link IndexWriter}s.
     *
     * @param writerConfigSupplier the supplier of writer configs
     * @return index builder
     */
    public LuceneSchemaIndexBuilder withWriterConfig( Supplier<IndexWriterConfig> writerConfigSupplier )
    {
        this.writerConfigSupplier = writerConfigSupplier;
        return this;
    }

    /**
     * Transform builder to build unique index
     *
     * @return index builder
     */
    public LuceneSchemaIndexBuilder uniqueIndex()
    {
        this.indexConfig = IndexConfiguration.UNIQUE;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    public LuceneSchemaIndex build()
    {
        return new LuceneSchemaIndex( storageBuilder.build(), indexConfig, samplingConfig, writerConfigSupplier );
    }

}
