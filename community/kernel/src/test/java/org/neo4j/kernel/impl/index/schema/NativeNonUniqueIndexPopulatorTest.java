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
package org.neo4j.kernel.impl.index.schema;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

@RunWith( Parameterized.class )
public class NativeNonUniqueIndexPopulatorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends NativeIndexPopulatorTests.NonUnique<KEY,VALUE>
{
    @Parameterized.Parameters( name = "{index} {0}" )
    public static Collection<Object[]> data()
    {
        return NativeIndexPopulatorTestCases.allCases();
    }

    @Parameterized.Parameter()
    public NativeIndexPopulatorTestCases.TestCase<KEY,VALUE> testCase;

    private static final StoreIndexDescriptor nonUniqueDescriptor = TestIndexDescriptorFactory.forLabel( 42, 666 ).withId( 0 );

    @Override
    NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException
    {
        return testCase.populatorFactory.create( pageCache, fs, getIndexFile(), layout, monitor, indexDescriptor, tokenNameLookup );
    }

    @Override
    ValueCreatorUtil<KEY,VALUE> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( nonUniqueDescriptor, testCase.typesOfGroup, ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexLayout<KEY,VALUE> createLayout()
    {
        return testCase.indexLayoutFactory.create();
    }
}
