/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.string.UTF8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.internal.progress.ThreadSafeMockingProgress.mockingProgress;

public class AuthTokenUtil
{
    @SuppressWarnings( "unchecked" )
    public static boolean matches( Map<String,Object> expected, Object actualObject )
    {
        if ( expected == null || actualObject == null )
        {
            return expected == actualObject;
        }

        if ( !(actualObject instanceof Map<?,?>) )
        {
            return false;
        }

        Map<String,Object> actual = (Map<String,Object>) actualObject;

        if ( expected.size() != actual.size() )
        {
            return false;
        }

        for ( Map.Entry<String,Object> expectedEntry : expected.entrySet() )
        {
            String key = expectedEntry.getKey();
            Object expectedValue = expectedEntry.getValue();
            Object actualValue = actual.get( key );
            if ( AuthToken.containsSensitiveInformation( key ) )
            {
                byte[] expectedByteArray = expectedValue instanceof byte[] ? (byte[]) expectedValue :
                                           expectedValue != null ? UTF8.encode( (String) expectedValue ) : null;
                if ( !Arrays.equals( expectedByteArray, (byte[]) actualValue ) )
                {
                    return false;
                }
            }
            else if ( expectedValue == null || actualValue == null )
            {
                return expectedValue == actualValue;
            }
            else if ( !expectedValue.equals( actualValue ) )
            {
                return false;
            }
        }
        return true;
    }

    public static void assertAuthTokenMatches( Map<String,Object> expected, Map<String,Object> actual )
    {
        assertFalse( expected == null ^ actual == null );
        assertEquals( expected.keySet(), actual.keySet() );
        expected.forEach( ( key, expectedValue ) ->
        {
            Object actualValue = actual.get( key );
            if ( AuthToken.containsSensitiveInformation( key ) )
            {
                byte[] expectedByteArray = expectedValue != null ? UTF8.encode( (String) expectedValue ) : null;
                assertTrue( Arrays.equals( expectedByteArray, (byte[]) actualValue ) );
            }
            else
            {
                assertEquals( expectedValue, actualValue );
            }
        } );
    }

    public static class AuthTokenMatcher extends BaseMatcher<Map<String,Object>>
    {
        private final Map<String,Object> expectedValue;

        public AuthTokenMatcher( Map<String,Object> expectedValue )
        {
            this.expectedValue = expectedValue;
        }

        @Override
        public boolean matches( Object o )
        {
            return AuthTokenUtil.matches( expectedValue, o );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendValue( this.expectedValue );
        }
    }

    public static AuthTokenMatcher authTokenMatcher( Map<String,Object> authToken )
    {
        return new AuthTokenMatcher( authToken );
    }

    public static class AuthTokenArgumentMatcher implements ArgumentMatcher<Map<String,Object>>, Serializable
    {

        private Map<String,Object> wanted;

        public AuthTokenArgumentMatcher( Map<String,Object> authToken )
        {
            this.wanted = authToken;
        }

        public boolean matches( Map<String,Object> actual )
        {
            return AuthTokenUtil.matches( wanted, actual );
        }

        public String toString()
        {
            return "authTokenArgumentMatcher(" + wanted + ")";
        }
    }

    public static Map<String,Object> authTokenArgumentMatcher( Map<String,Object> authToken )
    {
        mockingProgress().getArgumentMatcherStorage().reportMatcher( new AuthTokenArgumentMatcher( authToken ) );
        return null;
    }
}
