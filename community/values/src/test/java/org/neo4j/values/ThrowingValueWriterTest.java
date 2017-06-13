package org.neo4j.values;

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
        public void beginUTF8( int size ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void copyUTF8( long fromAddress, int length ) throws TestException
        {
            throw new TestException();
        }

        @Override
        public void endUTF8() throws TestException
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
    }
}
