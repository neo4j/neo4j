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
package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.ExplicitIndexValueValidator.INSTANCE;
import static org.neo4j.kernel.impl.api.LuceneIndexValueValidator.MAX_TERM_LENGTH;

class ExplicitIndexValueValidatorTest
{
    @Test
    void nullIsNotAllowed()
    {
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> INSTANCE.validate( null ) );
        assertEquals( iae.getMessage(), "Null value" );
    }

    @Test
    void stringOverExceedLimitNotAllowed()
    {
        int length = MAX_TERM_LENGTH * 2;
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> INSTANCE.validate( RandomStringUtils.randomAlphabetic( length ) ) );
        assertThat( iae.getMessage(), containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
    }

    @Test
    void nullToStringIsNotAllowed()
    {
        Object testValue = mock( Object.class );
        when( testValue.toString() ).thenReturn( null );
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> INSTANCE.validate( testValue ) );
        assertThat( iae.getMessage(), containsString( "has null toString" ) );
    }

    @Test
    void numberIsValidValue()
    {
        INSTANCE.validate( 5 );
        INSTANCE.validate( 5.0d );
        INSTANCE.validate( 5.0f );
        INSTANCE.validate( 5L );
    }

    @Test
    void shortStringIsValidValue()
    {
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 5 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 10 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 250 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 450 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( MAX_TERM_LENGTH ) );
    }
}
