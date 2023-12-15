/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import inet.ipaddr.IPAddressString;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.NullLog;

class WebURLAccessRuleTest {
    private AbstractSecurityLog securityLog = new CommunitySecurityLog(NullLog.getInstance());
    private final SecurityAuthorizationHandler securityAuthorizationHandler =
            new SecurityAuthorizationHandler(securityLog);

    @Test
    void shouldThrowWhenUrlIsWithinBlockedRange() throws Exception {
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
            final var error = assertThrows(URLAccessValidationError.class, () -> new WebURLAccessRule(config)
                    .validate(url, securityAuthorizationHandler, fullSecurityContext()));

            // assert that the validation fails
            assertThat(error.getMessage())
                    .contains("blocked via the configuration property internal.dbms.cypher_ip_blocklist");
        }
    }

    @Test
    void validationShouldPassWhenUrlIsNotWithinBlockedRange() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("132.0.0.0/8");
        Config config = mock(Config.class);
        when(config.get(GraphDatabaseInternalSettings.cypher_ip_blocklist)).thenReturn(List.of(blockedIpv4Range));
        final var urlAddresses = List.of(
                "http://localhost/test.csv",
                "https://localhost/test.csv",
                "ftp://localhost/test.csv",
                "http://[::1]/test.csv");

        final var expected = List.of(
                new URL("http://127.0.0.1/test.csv"),
                new URL("https://localhost/test.csv"),
                new URL("ftp://localhost/test.csv"),
                new URL("http://[::1]/test.csv"));

        for (int i = 0; i < urlAddresses.size(); i++) {
            final URL url = new URL(urlAddresses.get(i));

            // execute the query
            final var result = new WebURLAccessRule(config)
                    .checkNotBlockedAndPinToIP(url, securityAuthorizationHandler, fullSecurityContext());

            // assert that the validation passes and the IP is pinned
            assertThat(result).isEqualTo(expected.get(i));
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
        final var error = assertThrows(URLAccessValidationError.class, () -> new WebURLAccessRule(config)
                .validate(url, securityAuthorizationHandler, fullSecurityContext()));

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
        final var error = assertThrows(UnknownHostException.class, () -> new WebURLAccessRule(config)
                .validate(url, securityAuthorizationHandler, fullSecurityContext()));

        // assert that the validation fails
        // The error message is OS specific so only check that it fails on the expected host
        assertThat(error.getMessage()).contains("always.invalid");
    }

    @Test
    void shouldFailForRedirectedInvalidIps() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.0.0.1");

        // Mock a redirect (response code 302) from a valid URL (127.0.0.0) to an blocked URL (127.0.0.1)
        HttpURLConnection connection = mock(HttpURLConnection.class);
        when(connection.getResponseCode()).thenReturn(302);
        when(connection.getHeaderField("Location")).thenReturn("https://127.0.0.1");

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
        final var error = assertThrows(URLAccessValidationError.class, () -> new WebURLAccessRule(config)
                .validate(url, securityAuthorizationHandler, fullSecurityContext()));

        // assert that the validation fails
        assertThat(error.getMessage())
                .contains(
                        "access to /127.0.0.1 is blocked via the configuration property internal.dbms.cypher_ip_blocklist");
    }

    @Test
    void shouldNotFollowChangeInProtocols() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.168.0.1");

        // Mock a redirect (response code 306) from a valid URL (127.0.0.0) to another URL
        HttpURLConnection connection = mock(HttpURLConnection.class);
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
        final var urlConnection =
                new WebURLAccessRule(config).validate(url, securityAuthorizationHandler, fullSecurityContext());

        // assert that the url is the same, as the redirect was a change in protocol and shouldn't be followed
        assertEquals(urlConnection, connection);
    }

    @Test
    void shouldFailForExceedingRedirectLimit() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.168.0.1");

        // Mock a redirect (response code 302) from a valid URL (127.0.0.0) to an blocked URL (127.0.0.1)
        HttpURLConnection connectionA = mock(HttpURLConnection.class);
        when(connectionA.getResponseCode()).thenReturn(302);
        when(connectionA.getHeaderField("Location")).thenReturn("/b");

        HttpURLConnection connectionB = mock(HttpURLConnection.class);
        when(connectionB.getResponseCode()).thenReturn(302);
        when(connectionB.getHeaderField("Location")).thenReturn("/a");

        MutableInt counter = new MutableInt(0);
        URLStreamHandler urlStreamHandler = new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return counter.getAndIncrement() % 2 == 0 ? connectionA : connectionB;
            }
        };

        URL urlA = new URL("https", "127.0.0.0", 8000, "/a", urlStreamHandler);
        URL urlB = new URL("https", "127.0.0.0", 8000, "/b", urlStreamHandler);
        when(connectionA.getURL()).thenReturn(urlA);
        when(connectionB.getURL()).thenReturn(urlB);

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        // assert that the validation fails
        assertThatThrownBy(() -> new WebURLAccessRule(config)
                        .validate(urlA, securityAuthorizationHandler, fullSecurityContext()))
                .isInstanceOf(URLAccessValidationError.class)
                .hasMessageContaining("Redirect limit exceeded");
    }

    @Test
    void shouldPinIPsForHttpAndFtp() throws Exception {
        final IPAddressString blockedIpv4Range = new IPAddressString("127.168.0.1");
        final URL httpUrl = new URL("http://localhost/test.csv");
        final URL httpsUrl = new URL("https://localhost/test.csv");
        final URL ftpUrl = new URL("ftp://localhost/test.csv");

        // set the config
        final Config config =
                Config.defaults(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(blockedIpv4Range));

        class TestWebURLAccessRule extends WebURLAccessRule {

            public static boolean enteredIpPinning = false;

            public TestWebURLAccessRule() {
                super(config);
            }

            @Override
            protected URL substituteHostByIP(URL u, String ip) {
                enteredIpPinning = true;
                return u;
            }
        }
        TestWebURLAccessRule accessRule = new TestWebURLAccessRule();

        // execute the query
        assertThrows(
                ConnectException.class,
                () -> accessRule.validate(httpUrl, securityAuthorizationHandler, fullSecurityContext()));
        assertTrue(TestWebURLAccessRule.enteredIpPinning);

        TestWebURLAccessRule.enteredIpPinning = false;
        accessRule.validate(ftpUrl, securityAuthorizationHandler, fullSecurityContext());
        assertTrue(TestWebURLAccessRule.enteredIpPinning);

        TestWebURLAccessRule.enteredIpPinning = false;
        assertThrows(
                ConnectException.class,
                () -> accessRule.validate(httpsUrl, securityAuthorizationHandler, fullSecurityContext()));
        assertFalse(TestWebURLAccessRule.enteredIpPinning);
    }

    @Test
    void shouldSubstituteIpCorrectly() throws Exception {
        var accessRule = new WebURLAccessRule(Config.defaults());
        assertEquals(
                "http://127.0.0.1/test.csv",
                accessRule
                        .substituteHostByIP(new URL("http://localhost/test.csv"), "127.0.0.1")
                        .toString());
        assertEquals(
                "http://user:password@127.0.0.1/test.csv",
                accessRule
                        .substituteHostByIP(new URL("http://user:password@localhost/test.csv"), "127.0.0.1")
                        .toString());
        assertEquals(
                "https://user:password@127.0.0.1/test.csv?a=b&c=d",
                accessRule
                        .substituteHostByIP(new URL("https://user:password@localhost/test.csv?a=b&c=d"), "127.0.0.1")
                        .toString());
        assertEquals(
                "ftp://user:password@127.0.0.1/test.csv",
                accessRule
                        .substituteHostByIP(new URL("ftp://user:password@localhost/test.csv"), "127.0.0.1")
                        .toString());
    }

    private SecurityContext fullSecurityContext() {
        return new SecurityContext(
                AuthSubject.ANONYMOUS,
                AccessMode.Static.FULL,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                DEFAULT_DATABASE_NAME);
    }
}
