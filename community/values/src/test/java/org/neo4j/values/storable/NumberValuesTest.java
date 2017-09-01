/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.storable;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.NumberValues.hash;

public class NumberValuesTest
{

    @Test
    public void shouldHashNaN()
    {
        assertThat( hash( Double.NaN ), equalTo( Double.hashCode( Double.NaN ) ) );
    }

    @Test
    public void shouldHashInfinite()
    {
        assertThat( hash( Double.NEGATIVE_INFINITY ), equalTo( Double.hashCode( Double.NEGATIVE_INFINITY ) ) );
        assertThat( hash( Double.POSITIVE_INFINITY ), equalTo( Double.hashCode( Double.POSITIVE_INFINITY ) ) );
    }

    @Test
    public void shouldHashIntegralDoubleAsLong()
    {
        assertThat( hash( 1337d ), equalTo( hash( 1337L ) ) );
    }

}
