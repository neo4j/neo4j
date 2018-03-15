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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IGNORE;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;
import static org.neo4j.test.Randoms.CSA_LETTERS_AND_DIGITS;
import static org.neo4j.values.storable.Values.stringValue;

public class NativeStringIndexingIT
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, getClass() );

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldHandleKeySizesCloseToTheSizeLimit() throws IOException, IndexEntryConflictException, IndexNotApplicableKernelException
    {
        // given
        SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( 1, 2 );
        try ( StringSchemaIndexAccessor accessor = new StringSchemaIndexAccessor( storage.pageCache(), storage.fileSystem(),
                storage.directory().file( "index" ), new StringLayoutNonUnique(), IGNORE, EMPTY, descriptor, 0,
                new IndexSamplingConfig( Config.defaults() ) ) )
        {
            // when
            List<String> strings = new ArrayList<>();
            try ( NativeSchemaIndexUpdater<StringSchemaKey,NativeSchemaValue> updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
            {
                for ( int i = 0; i < 1_000; i++ )
                {
                    String string = random.string( 3_000, 4_000, CSA_LETTERS_AND_DIGITS );
                    strings.add( string );
                    updater.process( IndexEntryUpdate.add( i, descriptor, stringValue( string ) ) );
                }
            }

            // then
            try ( IndexReader reader = accessor.newReader() )
            {
                for ( int i = 0; i < strings.size(); i++ )
                {
                    try ( PrimitiveLongResourceIterator result = reader.query( exact( descriptor.schema().getPropertyIds()[0], strings.get( i ) ) ) )
                    {
                        assertEquals( i, single( result, -1 ) );
                    }
                }
            }
        }
    }
}
