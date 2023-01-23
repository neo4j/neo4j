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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import inet.ipaddr.IPAddressString;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;

class WebURLAccessRuleTest {
    @Test
    void shouldThrowWhenUrlIsWithinBlockedRange() throws MalformedURLException {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.0.0.0/8");
        final IPAddressString blockedIpv6Range = new IPAddressString("0:0:0:0:0:0:0:1/8");
        final var urlAddresses = List.of(
                "http://localhost/test.csv",
                "https://localhost/test.csv",
                "ftp://localhost/test.csv",
                "http://[::1]/test.csv");

        for (var urlAddress : urlAddresses) {
            final URL url = new URL(urlAddress);

            // set the config
            final Config config = Config.defaults(
                    GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range, blockedIpv6Range));

            // execute the query
            final var error = assertThrows(URLAccessValidationError.class, () -> URLAccessRules.webAccess()
                    .validate(config, url));

            // assert that the validation fails
            assertThat(error.getMessage())
                    .contains("blocked via the configuration property internal.dbms.cypher_ip_blocklist");
        }
    }

    @Test
    void validationShouldPassWhenUrlIsNotWithinBlockedRange() throws MalformedURLException, URLAccessValidationError {
        final var urlAddresses = List.of(
                "http://localhost/test.csv",
                "https://localhost/test.csv",
                "ftp://localhost/test.csv",
                "http://[::1]/test.csv");

        for (var urlAddress : urlAddresses) {
            final URL url = new URL(urlAddress);

            // set the config
            final Config config = Config.defaults();

            // execute the query
            final var result = URLAccessRules.webAccess().validate(config, url);

            // assert that the validation passes
            assert result == url;
        }
    }

    @Test
    void shouldWorkWithNonRangeIps() throws MalformedURLException {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.0.0.1");
        final URL url = new URL("http://localhost/test.csv");

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        // execute the query
        final var error = assertThrows(
                URLAccessValidationError.class, () -> URLAccessRules.webAccess().validate(config, url));

        // assert that the validation fails
        assertThat(error.getMessage())
                .contains("blocked via the configuration property internal.dbms.cypher_ip_blocklist");
    }

    @Test
    void shouldFailForInvalidIps() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.0.0.1");
        // The .invalid domain is always invalid, according to https://datatracker.ietf.org/doc/html/rfc2606#section-2
        final URL url = new URL("http://always.invalid/test.csv");

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        // execute the query
        final var error = assertThrows(
                URLAccessValidationError.class, () -> URLAccessRules.webAccess().validate(config, url));

        // assert that the validation fails
        assertThat(error.getMessage()).contains("Unable to verify access to always.invalid");
    }

    @Test
    void shouldFailForRedirectedInvalidIps() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.0.0.1");

        // Mock a redirect (response code 302) from a valid URL (127.0.0.0) to an blocked URL (127.0.0.1)
        HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
        when(connection.getResponseCode()).thenReturn(302);
        when(connection.getHeaderField("Location")).thenReturn("http://127.0.0.1");

        URLStreamHandler urlStreamHandler = new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return connection;
            }
        };

        URL url = new URL("http", "127.0.0.0", 8000, "", urlStreamHandler);

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        // execute the query
        final var error = assertThrows(
                URLAccessValidationError.class, () -> URLAccessRules.webAccess().validate(config, url));

        // assert that the validation fails
        assertThat(error.getMessage())
                .contains(
                        "access to /127.0.0.1 is blocked via the configuration property internal.dbms.cypher_ip_blocklist");
    }

    @Test
    void shouldNotFollowChangeInProtocols() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.168.0.1");

        // Mock a redirect (response code 306) from a valid URL (127.0.0.0) to another URL
        HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
        when(connection.getResponseCode()).thenReturn(306);
        when(connection.getHeaderField("Location")).thenReturn("http://127.0.0.1");

        URLStreamHandler urlStreamHandler = new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return connection;
            }
        };

        URL url = new URL("https", "127.0.0.0", 8000, "", urlStreamHandler);

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        // execute the query
        final var validatedURL = URLAccessRules.webAccess().validate(config, url);

        // assert that the url is the same, as the redirect was a change in protocol and shouldn't be followed
        assertEquals(url, validatedURL);
    }

    @Test
    void shouldFailForExceedingRedirectLimit() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.168.0.1");

        // Mock a redirect (response code 302) from a valid URL (127.0.0.0) to an blocked URL (127.0.0.1)
        HttpURLConnection connectionA = Mockito.mock(HttpURLConnection.class);
        when(connectionA.getResponseCode()).thenReturn(302);
        when(connectionA.getHeaderField("Location")).thenReturn("/b");

        HttpURLConnection connectionB = Mockito.mock(HttpURLConnection.class);
        when(connectionB.getResponseCode()).thenReturn(302);
        when(connectionB.getHeaderField("Location")).thenReturn("/a");

        MutableInt counter = new MutableInt(0);
        URLStreamHandler urlStreamHandler = new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return counter.getAndIncrement() % 2 == 0 ? connectionA : connectionB;
            }
        };

        URL urlA = new URL("http", "127.0.0.0", 8000, "/a", urlStreamHandler);
        URL urlB = new URL("http", "127.0.0.0", 8000, "/b", urlStreamHandler);
        when(connectionA.getURL()).thenReturn(urlA);
        when(connectionB.getURL()).thenReturn(urlB);

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        // assert that the validation fails
        assertThatThrownBy(() -> URLAccessRules.webAccess().validate(config, urlA))
                .isInstanceOf(URLAccessValidationError.class)
                .hasMessageContaining("Redirect limit exceeded");
    }
}
