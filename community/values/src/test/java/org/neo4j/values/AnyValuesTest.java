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
package org.neo4j.values;

import org.junit.Test;

import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class AnyValuesTest
{

    @Test
    public void shouldNotEqualVirtualValue()
    {
        VirtualValue virtual = new MyVirtualValue( 42 );

        assertNotEqual( booleanValue( false ), virtual );
        assertNotEqual( byteValue( (byte)0 ), virtual );
        assertNotEqual( shortValue( (short)0 ), virtual );
        assertNotEqual( intValue( 0 ), virtual );
        assertNotEqual( longValue( 0 ), virtual );
        assertNotEqual( floatValue( 0.0f ), virtual );
        assertNotEqual( doubleValue( 0.0 ), virtual );
        assertNotEqual( stringValue( "" ), virtual );
    }
}
