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
package org.neo4j.bolt.security.auth;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.kernel.api.security.AuthToken;

import static org.neo4j.helpers.collection.MapUtil.map;

public abstract class AuthTokenDecoderTest
{
    protected abstract void testShouldDecodeAuthToken( Map<String,Object> authToken ) throws Exception;

    protected abstract void testShouldFailToDecodeAuthToken( Map<String,Object> authToken, String expectedErrorMessage ) throws Exception;

    @Test
    void shouldDecodeAuthTokenWithStringCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, "password" ) );
    }

    @Test
    void shouldDecodeAuthTokenWithEmptyStringCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, "" ) );
    }

    @Test
    void shouldDecodeAuthTokenWithNullCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, null ) );
    }

    @Test
    void shouldDecodeAuthTokenWithStringNewCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, "password" ) );
    }

    @Test
    void shouldDecodeAuthTokenWithEmptyStringNewCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, "" ) );
    }

    @Test
    void shouldDecodeAuthTokenWithNullNewCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, null ) );
    }

    @Test
    void shouldFailToDecodeAuthTokenWithCredentialsOfUnsupportedTypes() throws Exception
    {
        for ( Object value : valuesWithInvalidTypes )
        {
            testShouldFailToDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, value ),
                    "INIT message authentication token field '" + AuthToken.CREDENTIALS + "' should be a UTF-8 encoded string" );
        }
    }

    @Test
    void shouldFailToDecodeAuthTokenWithNewCredentialsOfUnsupportedType() throws Exception
    {
        for ( Object value : valuesWithInvalidTypes )
        {
            testShouldFailToDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, value ),
                    "INIT message authentication token field '" + AuthToken.NEW_CREDENTIALS + "' should be a UTF-8 encoded string" );
        }
    }

    private static Map<String,Object> authTokenMapWith( String fieldName, Object fieldValue )
    {
        return map( AuthToken.PRINCIPAL, "neo4j", fieldName, fieldValue );
    }

    private static Object[] valuesWithInvalidTypes = {
            // This is not an exhaustive list
            new char[]{ 'p', 'a', 's', 's' },
            Collections.emptyList(),
            Collections.emptyMap()
    };
}
