/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.security;

import java.util.Collections;
import java.util.Map;

import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.neo4j.helpers.collection.MapUtil.map;

public interface AuthToken
{
    String SCHEME_KEY = "scheme";
    String PRINCIPAL = "principal";
    String CREDENTIALS = "credentials";
    String REALM_KEY = "realm";
    String PARAMETERS = "parameters";
    String NEW_CREDENTIALS = "new_credentials";
    String BASIC_SCHEME = "basic";
    String NATIVE_REALM = "native";

    static String safeCast( String key, Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        Object value = authToken.get( key );
        if ( value == null || !(value instanceof String) )
        {
            throw new InvalidAuthTokenException(
                    "The value associated with the key `" + key + "` must be a String but was: " +
                    (value == null ? "null" : value.getClass().getSimpleName()) );
        }
        return (String) value;
    }

    static Map<String,Object> safeCastMap( String key, Map<String,Object> authToken )
            throws InvalidAuthTokenException
    {
        Object value = authToken.get( key );
        if ( value == null )
        {
            return Collections.emptyMap();
        }
        else if ( value instanceof Map )
        {
            return (Map<String,Object>) value;
        }
        else
        {
            throw new InvalidAuthTokenException(
                    "The value associated with the key `" + key + "` must be a Map but was: " +
                    value.getClass().getSimpleName() );
        }
    }

    static Map<String,Object> newBasicAuthToken( String username, String password )
    {
        return map( AuthToken.SCHEME_KEY, BASIC_SCHEME, AuthToken.PRINCIPAL, username, AuthToken.CREDENTIALS,
                password );
    }

    static Map<String,Object> newBasicAuthToken( String username, String password, String realm )
    {
        return map( AuthToken.SCHEME_KEY, BASIC_SCHEME, AuthToken.PRINCIPAL, username, AuthToken.CREDENTIALS, password,
                AuthToken.REALM_KEY, realm );
    }

    static Map<String,Object> newCustomAuthToken( String principle, String credentials, String realm, String scheme )
    {
        return map( AuthToken.SCHEME_KEY, scheme, AuthToken.PRINCIPAL, principle, AuthToken.CREDENTIALS, credentials,
                AuthToken.REALM_KEY, realm );
    }

    static Map<String,Object> newCustomAuthToken( String principle, String credentials, String realm, String scheme,
            Map<String,Object> parameters )
    {
        return map( AuthToken.SCHEME_KEY, scheme, AuthToken.PRINCIPAL, principle, AuthToken.CREDENTIALS, credentials,
                AuthToken.REALM_KEY, realm, AuthToken.PARAMETERS, parameters );
    }
}
