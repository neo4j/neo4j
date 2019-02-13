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
    protected abstract void testShouldDecodeAuthToken( Map<String,Object> authToken, boolean checkDecodingResult ) throws Exception;

    @Test
    void shouldDecodeAuthTokenWithStringCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, "password" ), true );
    }

    @Test
    void shouldDecodeAuthTokenWithEmptyStringCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, "" ), true );
    }

    @Test
    void shouldDecodeAuthTokenWithNullCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.CREDENTIALS, null ), true );
    }

    @Test
    void shouldDecodeAuthTokenWithStringNewCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, "password" ), true );
    }

    @Test
    void shouldDecodeAuthTokenWithEmptyStringNewCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, "" ), true );
    }

    @Test
    void shouldDecodeAuthTokenWithNullNewCredentials() throws Exception
    {
        testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, null ), true );
    }

    @Test
    void shouldDecodeAuthTokenWithCredentialsOfUnsupportedTypes() throws Exception
    {
        for ( Object value : valuesWithInvalidTypes )
        {
            testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, value ), false );
        }
    }

    @Test
    void shouldDecodeAuthTokenWithNewCredentialsOfUnsupportedType() throws Exception
    {
        for ( Object value : valuesWithInvalidTypes )
        {
            testShouldDecodeAuthToken( authTokenMapWith( AuthToken.NEW_CREDENTIALS, value ), false );
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
