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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.containsString;
import static org.neo4j.kernel.impl.api.ExplicitIndexValueValidator.INSTANCE;
import static org.neo4j.kernel.impl.api.LuceneIndexValueValidator.MAX_TERM_LENGTH;

public class ExplicitIndexValueValidatorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void nullIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Null value" );
        INSTANCE.validate( null );
    }

    @Test
    public void stringOverExceedLimitNotAllowed()
    {
        int length = MAX_TERM_LENGTH * 2;
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( length ) );
    }

    @Test
    public void nullToStringIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "has null toString" );

        Object testValue = Mockito.mock( Object.class );
        Mockito.when( testValue.toString() ).thenReturn( null );
        INSTANCE.validate( testValue );
    }

    @Test
    public void numberIsValidValue()
    {
        INSTANCE.validate( 5 );
        INSTANCE.validate( 5.0d );
        INSTANCE.validate( 5.0f );
        INSTANCE.validate( 5L );
    }

    @Test
    public void shortStringIsValidValue()
    {
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 5 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 10 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 250 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( 450 ) );
        INSTANCE.validate( RandomStringUtils.randomAlphabetic( MAX_TERM_LENGTH ) );
    }
}
