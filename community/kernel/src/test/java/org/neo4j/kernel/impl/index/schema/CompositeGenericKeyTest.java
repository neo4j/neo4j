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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;

import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@ExtendWith( RandomExtension.class )
class CompositeGenericKeyTest
{
    @Inject
    RandomRule random;

    /**
     * This test verify that the documented formula for calculating size limit for string array
     * actually calculate correctly.
     */
    @Test
    void testDocumentedStringArrayKeySizeFormulaIsCorrect()
    {
        CompositeGenericKey key = new CompositeGenericKey( 1, mock( IndexSpecificSpaceFillingCurveSettingsCache.class ) );
        int maxArrayLength = random.nextInt( 500 );
        int maxStringLength = random.nextInt( 100 );
        for ( int i = 0; i < 100; i++ )
        {
            String[] strings = random.randomValues().nextStringArrayRaw( 0, maxArrayLength, 0, maxStringLength );
            key.initialize( i );
            key.writeValue( 0, Values.of( strings ), NativeIndexKey.Inclusion.NEUTRAL );
            assertThat( includingEntityId( calculateKeySize( strings ) ), equalTo( key.size() ) );
        }
    }

    private int includingEntityId( int keySize )
    {
        return Long.BYTES + keySize;
    }

    private int calculateKeySize( String[] strings )
    {
        int arrayLength = strings.length;
        int totalStringLength = 0;
        for ( String string : strings )
        {
            totalStringLength += string.getBytes( StandardCharsets.UTF_8 ).length;
        }
        return 1 + 2 + 2 * arrayLength + totalStringLength;
    }
}
