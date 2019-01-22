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
package org.neo4j.kernel.impl.store;

import org.junit.Test;

import java.util.Optional;

import org.neo4j.io.layout.DatabaseFile;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StoreTypeTest
{
    @Test
    public void storeTypeOfValidStoreFile()
    {
        StoreType matchedType = StoreType.typeOf( DatabaseFile.NODE_STORE ).orElseThrow( () -> new IllegalStateException( "Store type not found" ) );
        assertEquals( StoreType.NODE, matchedType );
    }

    @Test
    public void storeTypeOfMetaDataStoreFile()
    {
        StoreType matchedType = StoreType.typeOf( DatabaseFile.METADATA_STORE ).orElseThrow( () -> new IllegalStateException( "Store type not found" ) );
        assertEquals( StoreType.META_DATA, matchedType );
    }

    @Test
    public void storeTypeofSomeInvalidFile()
    {
        assertThat( StoreType.typeOf( DatabaseFile.LABEL_SCAN_STORE ), is( Optional.empty() ) );
    }
}
