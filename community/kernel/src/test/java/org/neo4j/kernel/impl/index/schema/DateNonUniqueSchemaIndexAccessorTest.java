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

import java.io.IOException;

import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;

public class DateNonUniqueSchemaIndexAccessorTest extends NativeSchemaIndexAccessorTest<DateSchemaKey,NativeSchemaValue>
{
    @Override
    NativeSchemaIndexAccessor<DateSchemaKey,NativeSchemaValue> makeAccessorWithSamplingConfig( IndexSamplingConfig samplingConfig ) throws IOException
    {
        TemporalIndexFiles.FileLayout<DateSchemaKey> fileLayout = new TemporalIndexFiles.FileLayout<>( getIndexFile(), layout, ValueGroup.DATE );
        return new TemporalIndexAccessor.PartAccessor<>( pageCache, fs, fileLayout, IMMEDIATE, monitor, indexDescriptor, samplingConfig );
    }

    @Override
    protected LayoutTestUtil<DateSchemaKey,NativeSchemaValue> createLayoutTestUtil()
    {
        return new DateLayoutTestUtil( IndexDescriptorFactory.forLabel( 42, 666 ) );
    }

}
