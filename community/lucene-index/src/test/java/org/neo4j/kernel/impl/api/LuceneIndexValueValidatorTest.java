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
package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.api.LuceneIndexValueValidator.INSTANCE;
import static org.neo4j.kernel.impl.api.LuceneIndexValueValidator.MAX_TERM_LENGTH;

class LuceneIndexValueValidatorTest
{
    @Test
    void tooLongArrayIsNotAllowed()
    {
        int length = MAX_TERM_LENGTH + 1;
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> getValidator().validate( RandomUtils.nextBytes( length ) ) );
        assertThat( iae.getMessage(), containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
    }

    @Test
    void stringOverExceedLimitNotAllowed()
    {
        int length = MAX_TERM_LENGTH * 2;
        IllegalArgumentException iae =
                assertThrows( IllegalArgumentException.class, () -> getValidator().validate( RandomStringUtils.randomAlphabetic( length ) ) );
        assertThat( iae.getMessage(), containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
    }

    @Test
    void nullIsNotAllowed()
    {
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> getValidator().validate( null ) );
        assertEquals( iae.getMessage(), "Null value" );
    }

    @Test
    void numberIsValidValue()
    {
        getValidator().validate( 5 );
        getValidator().validate( 5.0d );
        getValidator().validate( 5.0f );
        getValidator().validate( 5L );
    }

    @Test
    void shortArrayIsValidValue()
    {
        getValidator().validate( new long[] {1, 2, 3} );
        getValidator().validate( RandomUtils.nextBytes( 200 ) );
    }

    @Test
    void shortStringIsValidValue()
    {
        getValidator().validate( RandomStringUtils.randomAlphabetic( 5 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 10 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 250 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 450 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( MAX_TERM_LENGTH ) );
    }

    // Meant to be overridden for tests that want to verify the same things, but for a different validator
    private Validator<Object> getValidator()
    {
        return object -> INSTANCE.validate( Values.of( object ) );
    }
}
