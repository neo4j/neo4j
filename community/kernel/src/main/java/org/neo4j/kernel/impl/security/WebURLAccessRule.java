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

import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;

import inet.ipaddr.IPAddressString;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.security.URLAccessValidationError;

public class WebURLAccessRule implements URLAccessRule, URLAccessChecker {
    public static final String LOAD_CSV_USER_AGENT_PREFIX = "NeoLoadCSV_";
    private static final int REDIRECT_LIMIT = 10;
    private final Configuration config;

    public WebURLAccessRule(Configuration config) {

        this.config = config;
    }

    public static String userAgent() {
        var version = Runtime.version();
        var agent = System.getProperty("http.agent");
        if (agent == null) {
            return "Java/" + version;
        }
        return agent + " Java/" + version;
    }

    @Override
    public URL checkURL(URL url) throws URLAccessValidationError {
        List<IPAddressString> blockedIpRanges = config.get(GraphDatabaseInternalSettings.cypher_ip_blocklist);
        try {
            return checkNotBlockedAndPinToIP(url, blockedIpRanges);
        } catch (Exception e) {
            if (e instanceof URLAccessValidationError) {
                throw (URLAccessValidationError) e;
            } else {
                throw new URLAccessValidationError(
                        "Unable to verify access to " + url.getHost() + ". Cause: " + e.getMessage());
            }
        }
    }

    // This is used by APOC and thus needs to be public
    public URL checkNotBlockedAndPinToIP(URL url, List<IPAddressString> blockedIpRanges) throws Exception {
        InetAddress inetAddress = InetAddress.getByName(url.getHost());
        URL result = url;

        for (var blockedIpRange : blockedIpRanges) {
            if (blockedIpRange.contains(new IPAddressString(inetAddress.getHostAddress()))) {
                throw new URLAccessValidationError(
                        "access to " + inetAddress + " is blocked via the configuration property "
                                + GraphDatabaseInternalSettings.cypher_ip_blocklist.name());
            }
        }

        // If the address is a http or ftp one, we want to avoid an extra DNS lookup to avoid
        // DNS spoofing. It is unlikely, but it could happen between the first DNS resolve above
        // and the con.connect() below, in case we have the JVM dns cache disabled, or it
        // expires in between this two calls. Thus, we substitute the resolved ip here
        //
        // In the case of https DNS spoofing is not possible. Source here:
        // https://security.stackexchange.com/questions/94331/why-doesnt-dns-spoofing-work-against-https-sites
        if (url.getProtocol().equals("http") || url.getProtocol().equals("ftp")) {
            result = substituteHostByIP(url, inetAddress.getHostAddress());
        }

        return result;
    }

    protected URL substituteHostByIP(URL u, String ip) throws MalformedURLException {
        String s;
        int port;
        String newURLString = u.getProtocol() + "://"
                + ((s = u.getUserInfo()) != null && !s.isEmpty() ? s + '@' : "")
                + ((s = u.getHost()) != null && !s.isEmpty() ? ip : "")
                + ((port = u.getPort()) != u.getDefaultPort() && port > 0 ? ':' + Integer.toString(port) : "")
                + ((s = u.getPath()) != null ? s : "")
                + ((s = u.getQuery()) != null ? '?' + s : "")
                + ((s = u.getRef()) != null ? '#' + s : "");

        return new URL(newURLString);
    }

    public URLConnection checkUrlIncludingHops(URL url, List<IPAddressString> blockedIpRanges) throws Exception {
        URL result = url;
        boolean keepFollowingRedirects;
        int redirectLimit = REDIRECT_LIMIT;
        URLConnection urlCon;
        HttpURLConnection httpCon;

        do {
            // We need to validate each intermediate url if there are redirects.
            // Otherwise, we could have situations like an internal ip, e.g. 10.0.0.1
            // is banned in the config, but it redirects to another different internal ip
            // and we would still have a security hole
            result = checkNotBlockedAndPinToIP(result, blockedIpRanges);
            urlCon = result.openConnection();

            if (urlCon instanceof HttpURLConnection) {
                httpCon = (HttpURLConnection) urlCon;
                httpCon.setRequestProperty(
                        "User-Agent", String.format("%s%s", LOAD_CSV_USER_AGENT_PREFIX, userAgent()));
                httpCon.setInstanceFollowRedirects(false);
                httpCon.connect();
                httpCon.getInputStream();
                keepFollowingRedirects = isRedirect(httpCon.getResponseCode());

                if (keepFollowingRedirects) {
                    if (redirectLimit-- == 0) {
                        httpCon.disconnect();
                        throw new IOException("Redirect limit exceeded");
                    }
                    String location = httpCon.getHeaderField("Location");

                    if (location == null) {
                        httpCon.disconnect();
                        throw new IOException("URL responded with a redirect but the location header was null");
                    }

                    URL newUrl;
                    try {
                        newUrl = new URL(location);
                        if (!newUrl.getProtocol().equalsIgnoreCase(result.getProtocol())) {
                            return httpCon;
                        }
                    } catch (MalformedURLException e) {
                        // Try to use the location as a relative path, matches browser behaviour
                        newUrl = new URL(httpCon.getURL(), location);
                    }
                    result = newUrl;
                }
            } else {
                keepFollowingRedirects = false;
            }
        } while (keepFollowingRedirects);

        return urlCon;
    }

    public static boolean isRedirect(int responseCode) {
        return responseCode >= 300 && responseCode <= 307 && responseCode != 306 && responseCode != HTTP_NOT_MODIFIED;
    }

    @Override
    public URL validate(Configuration config, URL url) throws URLAccessValidationError {
        List<IPAddressString> blockedIpRanges = config.get(GraphDatabaseInternalSettings.cypher_ip_blocklist);
        String host = url.getHost();
        if (!blockedIpRanges.isEmpty() && host != null && !host.isEmpty()) {
            try {
                URLConnection con = checkUrlIncludingHops(url, blockedIpRanges);
                if (con instanceof HttpURLConnection) {
                    ((HttpURLConnection) con).disconnect();
                }
            } catch (Exception e) {
                throw new URLAccessValidationError("Unable to verify access to " + host + ". Cause: " + e.getMessage());
            }
        }
        return url;
    }
}
