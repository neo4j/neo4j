/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.convert;

import org.junit.Test;

import org.neo4j.kernel.impl.store.StoreId;

import static org.junit.Assert.*;

public class ConversionMetaDataTest
{
    @Test
    public void shouldMarshalAndUnmarshalConversionId() throws Exception
    {
        // given
        StoreId before = new StoreId( 1, 2, 3, 4, 5 );
        StoreId after = new StoreId( 6, 7, 8, 9, 10 );
        long transactionId = 44L;

        SourceMetadata initialMetadata = new SourceMetadata( before, after, transactionId );

        // when
        String conversionId = initialMetadata.getConversionId();

        SourceMetadata expectedMetadata = SourceMetadata.create( conversionId );

        // then
        assertEquals( expectedMetadata, initialMetadata );
    }
}
