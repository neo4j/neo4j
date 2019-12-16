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

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.api.LuceneIndexValueValidator.MAX_TERM_LENGTH;
import static org.neo4j.values.storable.Values.of;

class LuceneIndexValueValidatorTest
{
    private static final IndexDescriptor descriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 1 ) ).withName( "test" ).materialise( 1 );
    private static final IndexValueValidator VALIDATOR = new LuceneIndexValueValidator( descriptor );

    @Test
    void tooLongArrayIsNotAllowed()
    {
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> {
            TextArray largeArray = Values.stringArray( randomAlphabetic( MAX_TERM_LENGTH ), randomAlphabetic( MAX_TERM_LENGTH ) );
            VALIDATOR.validate( largeArray );
        } );
        assertThat( iae.getMessage() ).contains( "Property value is too large to index" );
    }

    @Test
    void stringOverExceedLimitNotAllowed()
    {
        int length = MAX_TERM_LENGTH + 1;
        IllegalArgumentException iae =
                assertThrows( IllegalArgumentException.class, () -> VALIDATOR.validate( values( randomAlphabetic( length ) ) ) );
        assertThat( iae.getMessage() ).contains( "Property value is too large to index" );
    }

    @Test
    void nullIsNotAllowed()
    {
        IllegalArgumentException iae = assertThrows( IllegalArgumentException.class, () -> VALIDATOR.validate( values( (Object) null ) ) );
        assertEquals( iae.getMessage(), "Null value" );
    }

    @Test
    void numberIsValidValue()
    {
        VALIDATOR.validate( values( 5 ) );
        VALIDATOR.validate( values( 5.0d ) );
        VALIDATOR.validate( values( 5.0f ) );
        VALIDATOR.validate( values( 5L ) );
    }

    @Test
    void shortArrayIsValidValue()
    {
        VALIDATOR.validate( values( (Object) new long[] {1, 2, 3} ) );
        VALIDATOR.validate( values( (Object) RandomUtils.nextBytes( 200 ) ) );
    }

    @Test
    void shortStringIsValidValue()
    {
        VALIDATOR.validate( values( randomAlphabetic( 5 ) ) );
        VALIDATOR.validate( values( randomAlphabetic( 10 ) ) );
        VALIDATOR.validate( values( randomAlphabetic( 250 ) ) );
        VALIDATOR.validate( values( randomAlphabetic( 450 ) ) );
        VALIDATOR.validate( values( randomAlphabetic( MAX_TERM_LENGTH ) ) );
    }

    private Value[] values( Object... objects )
    {
        Value[] array = new Value[objects.length];
        for ( int i = 0; i < objects.length; i++ )
        {
            array[i] = of( objects[i] );
        }
        return array;
    }
}
