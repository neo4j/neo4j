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
package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.neo4j.kernel.impl.api.IndexValueLengthValidator.INSTANCE;

public class IndexValueLengthValidatorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void tooLongByteArrayIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Property value bytes length: 35000 is longer then 32766, " +
                "which is maximum supported length of indexed property value." );
        INSTANCE.validate( RandomUtils.nextBytes( 35000 ) );
    }

    @Test
    public void stringOverExceedLimitNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage(
                "Property value bytes length: 32767 is longer then 32766, " +
                        "which is maximum supported length of indexed property value." );
        INSTANCE.validate( RandomUtils.nextBytes( IndexValueLengthValidator.MAX_TERM_LENGTH + 1 ) );
    }

    @Test
    public void shortByteArrayIsValid()
    {
        INSTANCE.validate( RandomUtils.nextBytes( 3 ) );
        INSTANCE.validate( RandomUtils.nextBytes( 30 ) );
        INSTANCE.validate( RandomUtils.nextBytes( 300 ) );
        INSTANCE.validate( RandomUtils.nextBytes( 4303 ) );
        INSTANCE.validate( RandomUtils.nextBytes( 13234 ) );
        INSTANCE.validate( RandomUtils.nextBytes( IndexValueLengthValidator.MAX_TERM_LENGTH ) );
    }

}
