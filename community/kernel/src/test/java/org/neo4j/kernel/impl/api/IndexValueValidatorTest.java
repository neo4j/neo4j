/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.values.storable.Values;

public class IndexValueValidatorTest extends IndexSimpleValueValidatorTest
{
    @Test
    public void tooLongArrayIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "is longer than 32766, " +
                         "which is maximum supported length of indexed property value."  );
        getValidator().validate( RandomUtils.nextBytes(39978 ) );
    }

    @Test
    public void shortArrayIsAllowed()
    {
        getValidator().validate( RandomUtils.nextBytes( 3 ) );
        getValidator().validate( RandomUtils.nextBytes( 30 ) );
        getValidator().validate( RandomUtils.nextBytes( 450 ) );
        getValidator().validate( RandomUtils.nextBytes( 4556 ) );
    }

    @Override
    protected Validator<Object> getValidator()
    {
        return object -> IndexValueValidator.INSTANCE.validate( Values.of( object ) );
    }
}
