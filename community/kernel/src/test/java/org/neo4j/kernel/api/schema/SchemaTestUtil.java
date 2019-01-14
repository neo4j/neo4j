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
package org.neo4j.kernel.api.schema;

import org.neo4j.internal.kernel.api.TokenNameLookup;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class SchemaTestUtil
{
    private SchemaTestUtil()
    {
    }

    public static void assertEquality( Object o1, Object o2 )
    {
        assertTrue( o1.getClass().getSimpleName() + "s are not equal", o1.equals( o2 ) );
        assertTrue( o1.getClass().getSimpleName() + "s do not have the same hashcode",
                o1.hashCode() == o2.hashCode() );
    }

    public static void assertArray( int[] values, int... expected )
    {
        assertThat( values.length, equalTo( expected.length ) );
        for ( int i = 0; i < values.length; i++ )
        {
            assertTrue( format( "Expected %d, got %d at index %d", expected[i], values[i], i ),
                    values[i] == expected[i] );
        }
    }

    public static TokenNameLookup simpleNameLookup = new TokenNameLookup()
    {
        @Override
        public String labelGetName( int labelId )
        {
            return "Label" + labelId;
        }

        @Override
        public String relationshipTypeGetName( int relationshipTypeId )
        {
            return "RelType" + relationshipTypeId;
        }

        @Override
        public String propertyKeyGetName( int propertyKeyId )
        {
            return "property" + propertyKeyId;
        }
    };
}
