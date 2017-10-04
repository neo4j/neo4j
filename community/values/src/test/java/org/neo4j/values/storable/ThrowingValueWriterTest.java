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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ThrowingValueWriterTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBeAbleToThrowFromValueWriter() throws TestException
    {
        // Given
        Value value = Values.of( "This is a value" );
        ThrowingValueWriter writer = new ThrowingValueWriter();

        // Expect
        exception.expect( TestException.class );

        // When
        value.writeTo( writer );
    }

    private static class TestException extends Exception
    {
    }

    private static class ThrowingValueWriter implements ValueWriter<TestException>
    {

        @Override
        public void writeNull() throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeBoolean( boolean value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeInteger( byte value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeInteger( short value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeInteger( int value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeInteger( long value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeFloatingPoint( float value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeFloatingPoint( double value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeString( String value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeString( char value ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeString( char[] value, int offset, int length ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void beginArray( int size, ArrayType arrayType ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void endArray() throws TestException
        {
            throw new TestException();
        }

        @Override
        public void writeByteArray( byte[] value ) throws TestException
        {
            throw new TestException();
        }
    }
}
