/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.security;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

class WebURLAccessRuleTest
{
    @Test
    void shouldThrowWhenUrlIsWithinBlockedRange() throws MalformedURLException
    {
        final List<String> urlAddresses = Arrays.asList( "http://localhost/test.csv", "https://localhost/test.csv", "ftp://localhost/test.csv", "http://[::1]/test.csv" );

        for ( String urlAddress : urlAddresses )
        {
            final URL url = new URL( urlAddress );

            //set the config
            final Config config = Config.defaults( stringMap( GraphDatabaseSettings.cypher_ip_blocklist.name(), "127.0.0.0/8,0:0:0:0:0:0:0:1/8"));

            //execute the query
            final URLAccessValidationError error = assertThrows( URLAccessValidationError.class, () ->
                    URLAccessRules.webAccess().validate( config, url ) );

            //assert that the validation fails
            assertThat( error.getMessage(), containsString( "blocked via the configuration property unsupported.dbms.cypher_ip_blocklist" ));
        }
    }

    @Test
    void validationShouldPassWhenUrlIsNotWithinBlockedRange() throws MalformedURLException, URLAccessValidationError
    {
        final List<String> urlAddresses = Arrays.asList( "http://localhost/test.csv", "https://localhost/test.csv", "ftp://localhost/test.csv", "http://[::1]/test.csv" );

        for ( String urlAddress : urlAddresses )
        {
            final URL url = new URL( urlAddress );

            //set the config
            final Config config = Config.defaults();

            //execute the query
            final URL result = URLAccessRules.webAccess().validate( config, url );

            //assert that the validation passes
            assert result == url;
        }
    }

    @Test
    void shouldWorkWithNonRangeIps() throws MalformedURLException
    {
        final URL url = new URL( "http://localhost/test.csv" );

        //set the config
        final Config config = Config.defaults( stringMap( GraphDatabaseSettings.cypher_ip_blocklist.name(), "127.0.0.1"));

        //execute the query
        final URLAccessValidationError error = assertThrows( URLAccessValidationError.class, () ->
                URLAccessRules.webAccess().validate( config, url ) );

        //assert that the validation fails
        assertThat( error.getMessage(), containsString( "blocked via the configuration property unsupported.dbms.cypher_ip_blocklist" ));
    }

    @Test
    void shouldFailForInvalidIps() throws Exception
    {
        // The .invalid domain is always invalid, according to https://datatracker.ietf.org/doc/html/rfc2606#section-2
        final URL url = new URL( "http://always.invalid/test.csv" );

        //set the config
        final Config config = Config.defaults( stringMap( GraphDatabaseSettings.cypher_ip_blocklist.name(), "127.0.0.1"));

        //execute the query
        final URLAccessValidationError error = assertThrows( URLAccessValidationError.class, () ->
                URLAccessRules.webAccess().validate( config, url ) );

        //assert that the validation fails
        assertThat( error.getMessage(), containsString( "Unable to verify access to always.invalid" ));
    }
}
