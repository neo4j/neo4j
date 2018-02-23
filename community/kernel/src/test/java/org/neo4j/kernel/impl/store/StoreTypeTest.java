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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class StoreTypeTest
{
    @Test
    public void storeTypeOfValidStoreFile()
    {
        //noinspection OptionalGetWithoutIsPresent
        assertEquals( StoreType.NODE, StoreType.typeOf( "neostore.nodestore.db" ).get() );
    }

    @Test
    public void storeTypeOfMetaDataStoreFile()
    {
        //noinspection OptionalGetWithoutIsPresent
        String fileName = MetaDataStore.DEFAULT_NAME;
        assertEquals( StoreType.META_DATA, StoreType.typeOf( fileName ).get() );
    }

    @Test
    public void storeTypeofSomeInvalidFile()
    {
        assertThat( StoreType.typeOf( "test.txt" ), is( Optional.empty() ) );
    }
}
