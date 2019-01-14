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
package org.neo4j.helpers;

import org.junit.Test;

import org.neo4j.function.Predicates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestExceptions
{
    @Test
    public void canPeelExceptions()
    {
        // given
        Throwable expected;
        Throwable exception =
                new LevelOneException( "",
                        new LevelTwoException( "",
                                new LevelThreeException( "",
                                        expected = new LevelThreeException( "include",
                                                new LevelFourException( "" ) ) ) ) );

        // when
        Throwable peeled = Exceptions.peel( exception,
                item -> !(item instanceof LevelThreeException) || !item.getMessage().contains( "include" ) );

        // then
        assertEquals( expected, peeled );
    }

    @Test
    public void canPeelUsingConveniencePredicate()
    {
        // given
        Throwable expected;
        Throwable exception =
                new ARuntimeException(
                        new AnotherRuntimeException(
                                new LevelFourException( "",
                                        expected = new LevelThreeException( "",
                                                new LevelFourException( "" ) ) ) ) );

        // when
        Throwable peeled = Exceptions.peel( exception,
                Predicates.instanceOfAny( RuntimeException.class, LevelFourException.class ) );

        // then
        assertEquals( expected, peeled );
    }

    @Test
    public void shouldDetectContainsOneOfSome()
    {
        // GIVEN
        Throwable cause = new ARuntimeException( new AnotherRuntimeException( new NullPointerException( "Some words" ) ) );

        // THEN
        assertTrue( Exceptions.contains( cause, NullPointerException.class ) );
        assertTrue( Exceptions.contains( cause, "words", NullPointerException.class ) );
        assertFalse( Exceptions.contains( cause, "not", NullPointerException.class ) );
    }

    @Test
    public void shouldSetMessage()
    {
        // GIVEN
        String initialMessage = "Initial message";
        LevelOneException exception = new LevelOneException( initialMessage );

        // WHEN
        String prependedMessage = "Prepend this: " + exception.getMessage();
        Exceptions.withMessage( exception, prependedMessage );

        // THEN
        assertEquals( prependedMessage, exception.getMessage() );
    }

    private static class LevelOneException extends Exception
    {
        LevelOneException( String message )
        {
            super( message );
        }

        LevelOneException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static class LevelTwoException extends LevelOneException
    {
        LevelTwoException( String message )
        {
            super( message );
        }

        LevelTwoException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static class LevelThreeException extends LevelTwoException
    {
        LevelThreeException( String message )
        {
            super( message );
        }

        LevelThreeException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static class LevelFourException extends LevelThreeException
    {
        LevelFourException( String message )
        {
            super( message );
        }

        LevelFourException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static class ARuntimeException extends RuntimeException
    {
        ARuntimeException( Throwable cause )
        {
            super( cause );
        }
    }

    private static class AnotherRuntimeException extends RuntimeException
    {
        AnotherRuntimeException( Throwable cause )
        {
            super( cause );
        }
    }
}
