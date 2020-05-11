/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.index;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class MinimalIndexAccessorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    private MinimalIndexAccessor minimalIndexAccessor;

    public MinimalIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( 1, 2 ) ) );
    }

    @Before
    public void before()
    {
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        IndexPopulator populator = indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ), INSTANCE );
        populator.create();
        populator.close( true, NULL );
        minimalIndexAccessor = indexProvider.getMinimalIndexAccessor( descriptor );
    }

    @Test
    public void indexDropperMustDropIndex()
    {
        // given
        File rootDirectory = indexProvider.directoryStructure().rootDirectory();
        File[] files = fs.listFiles( rootDirectory );
        assertEquals( 1, files.length );

        // when
        minimalIndexAccessor.drop();

        // then
        files = fs.listFiles( rootDirectory );
        assertEquals( 0, files.length );
    }

    @Test
    public void indexDropperMustProvideIndexConfiguration()
    {
        // when
        Map<String,Value> dropperConfiguration = minimalIndexAccessor.indexConfig();

        // then
        assertEquals( descriptor.getIndexConfig().asMap(), dropperConfiguration );
    }
}
