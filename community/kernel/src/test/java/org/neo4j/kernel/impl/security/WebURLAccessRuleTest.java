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

import inet.ipaddr.IPAddressString;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WebURLAccessRuleTest
{
    @Test
    void shouldThrowWhenUrlIsWithinBlockedRange() throws MalformedURLException
    {
        final IPAddressString blockedIpv4Range = new IPAddressString( "127.0.0.0/8" );
        final IPAddressString blockedIpv6Range = new IPAddressString( "0:0:0:0:0:0:0:1/8" );
        final var urlAddresses = List.of( "http://localhost/test.csv", "https://localhost/test.csv", "ftp://localhost/test.csv", "http://[::1]/test.csv" );

        for ( var urlAddress : urlAddresses )
        {
            final URL url = new URL( urlAddress );

            //set the config
            final Config config = Config.defaults( GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of( blockedIpv4Range, blockedIpv6Range ) );

            //execute the query
            final var error = assertThrows( URLAccessValidationError.class, () ->
                    URLAccessRules.webAccess().validate( config, url ) );

            //assert that the validation fails
            assertThat( error.getMessage() ).contains( "blocked via the configuration property unsupported.dbms.cypher_ip_blocklist" );
        }
    }

    @Test
    void validationShouldPassWhenUrlIsNotWithinBlockedRange() throws MalformedURLException, URLAccessValidationError
    {
        final var urlAddresses = List.of( "http://localhost/test.csv", "https://localhost/test.csv", "ftp://localhost/test.csv", "http://[::1]/test.csv" );

        for ( var urlAddress : urlAddresses )
        {
            final URL url = new URL( urlAddress );

            //set the config
            final Config config = Config.defaults();

            //execute the query
            final var result = URLAccessRules.webAccess().validate( config, url );

            //assert that the validation passes
            assert result == url;
        }
    }

    @Test
    void shouldWorkWithNonRangeIps() throws MalformedURLException
    {
        final IPAddressString blockedIpv4Range = new IPAddressString( "127.0.0.1" );
        final URL url = new URL( "http://localhost/test.csv" );

        //set the config
        final Config config = Config.defaults( GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of( blockedIpv4Range ) );

        //execute the query
        final var error = assertThrows( URLAccessValidationError.class, () ->
                URLAccessRules.webAccess().validate( config, url ) );

        //assert that the validation fails
        assertThat( error.getMessage() ).contains( "blocked via the configuration property unsupported.dbms.cypher_ip_blocklist" );
    }

    @Test
    void shouldFailForInvalidIps() throws Exception
    {
        final IPAddressString blockedIpv4Range = new IPAddressString( "127.0.0.1" );
        // The .invalid domain is always invalid, according to https://datatracker.ietf.org/doc/html/rfc2606#section-2
        final URL url = new URL( "http://always.invalid/test.csv" );

        //set the config
        final Config config = Config.defaults( GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of( blockedIpv4Range ) );

        //execute the query
        final var error = assertThrows( URLAccessValidationError.class, () ->
                URLAccessRules.webAccess().validate( config, url ) );

        //assert that the validation fails
        assertThat( error.getMessage() ).contains( "Unable to verify access to always.invalid" );
    }
}
