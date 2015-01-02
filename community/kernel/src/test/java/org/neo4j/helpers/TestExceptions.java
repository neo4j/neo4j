/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.helpers;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestExceptions
{
    @Test
    public void canPeelExceptions() throws Exception
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
        Throwable peeled = Exceptions.peel( exception, new Predicate<Throwable>()
        {
            @Override
            public boolean accept( Throwable item )
            {
                return !(item instanceof LevelThreeException) || !item.getMessage().contains( "include" );
            }
        } );

        // then
        assertEquals( expected, peeled );
    }
    
    @Test
    public void canPeelUsingConveniencePredicate() throws Exception
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
        Throwable peeled = Exceptions.peel( exception, Exceptions.exceptionsOfType( RuntimeException.class, LevelFourException.class ) );
        
        // then
        assertEquals( expected, peeled );
    }
    
    @Test
    public void shouldDetectContainsOneOfSome() throws Exception
    {
        // GIVEN
        Throwable cause = new ARuntimeException( new AnotherRuntimeException( new NullPointerException( "Some words" ) ) );
        
        // THEN
        assertTrue( Exceptions.contains( cause, NullPointerException.class ) );
        assertTrue( Exceptions.contains( cause, "words", NullPointerException.class ) );
        assertFalse( Exceptions.contains( cause, "not", NullPointerException.class ) );
    }

    private static class LevelOneException extends Exception
    {
        public LevelOneException( String message )
        {
            super( message );
        }

        public LevelOneException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }
    
    private static class LevelTwoException extends LevelOneException
    {
        public LevelTwoException( String message )
        {
            super( message );
        }

        public LevelTwoException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static class LevelThreeException extends LevelTwoException
    {
        public LevelThreeException( String message )
        {
            super( message );
        }

        public LevelThreeException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static class LevelFourException extends LevelThreeException
    {
        public LevelFourException( String message )
        {
            super( message );
        }

        public LevelFourException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }
    
    private static class ARuntimeException extends RuntimeException
    {
        public ARuntimeException( Throwable cause )
        {
            super( cause );
        }
    }

    private static class AnotherRuntimeException extends RuntimeException
    {
        public AnotherRuntimeException( Throwable cause )
        {
            super( cause );
        }
    }
}
