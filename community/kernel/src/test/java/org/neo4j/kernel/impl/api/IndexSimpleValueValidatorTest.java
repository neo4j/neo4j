/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.impl.util.Validator;

import static org.neo4j.kernel.impl.api.IndexSimpleValueValidator.INSTANCE;

public class IndexSimpleValueValidatorTest
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void nullIsNotAllowed() throws Exception
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Null value" );
        getValidator().validate( null );
    }

    @Test
    public void tooLongStringIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage(
                "Property value bytes length: 35000 is longer then 32766, " +
                        "which is maximum supported length of indexed property value." );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 35000 ) );
    }

    @Test
    public void stringOverExceedLimitNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage(
                "Property value bytes length: 32767 is longer then 32766, " +
                        "which is maximum supported length of indexed property value." );
        getValidator().validate( RandomStringUtils.randomAlphabetic( IndexValueLengthValidator.MAX_TERM_LENGTH + 1 ) );
    }

    @Test
    public void numberIsValidValue()
    {
        getValidator().validate( 5 );
        getValidator().validate( 5.0d );
        getValidator().validate( 5.0f );
        getValidator().validate( 5L );
    }

    @Test
    public void shortStringIsValidValue()
    {
        getValidator().validate( RandomStringUtils.randomAlphabetic( 5 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 10 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 250 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( 450 ) );
        getValidator().validate( RandomStringUtils.randomAlphabetic( IndexValueLengthValidator.MAX_TERM_LENGTH ) );
    }

    protected Validator<Object> getValidator()
    {
        return INSTANCE;
    }
}
