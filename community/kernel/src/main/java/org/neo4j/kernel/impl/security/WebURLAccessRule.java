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
import java.io.InputStream;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpURLConnection;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.Readables;
import org.neo4j.exceptions.LoadExternalResourceException;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

public class WebURLAccessRule implements AccessRule<URL> {
    public static final String LOAD_CSV_USER_AGENT_PREFIX = "NeoLoadCSV_";
    private static final int REDIRECT_LIMIT = 10;
    private final Configuration config;
    public static final int CONNECTION_TIMEOUT = 2000;
    public static final int READ_TIMOUT = 10 * 60 * 1000;
    private static final CookieManager cookieManager;

    static {
        cookieManager = new CookieManager();
        CookieHandler.setDefault(cookieManager);
        cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
    }

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

    URL checkNotBlockedAndPinToIP(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws UnknownHostException, MalformedURLException, URISyntaxException, URLAccessValidationError {

        // Keep doing this for community, and for backward compatibility with users (and tests) that don't
        // have security enabled.
        List<IPAddressString> blockedIpRanges = config.get(GraphDatabaseInternalSettings.cypher_ip_blocklist);
        InetAddress inetAddress = InetAddress.getByName(url.getHost());
        for (var blockedIpRange : blockedIpRanges) {
            if (blockedIpRange.contains(new IPAddressString(inetAddress.getHostAddress()))) {
                throw new URLAccessValidationError(
                        "access to " + inetAddress + " is blocked via the configuration property "
                                + GraphDatabaseInternalSettings.cypher_ip_blocklist.name());
            }
        }

        // RBAC security check
        securityAuthorizationHandler.assertLoadAllowed(securityContext, url.toURI(), inetAddress);

        // If the address is a http or ftp one, we want to avoid an extra DNS lookup to avoid
        // DNS spoofing. It is unlikely, but it could happen between the first DNS resolve above
        // and the con.connect() below, in case we have the JVM dns cache disabled, or it
        // expires in between this two calls. Thus, we substitute the resolved ip here
        //
        // In the case of https DNS spoofing is not possible. Source here:
        // https://security.stackexchange.com/questions/94331/why-doesnt-dns-spoofing-work-against-https-sites
        URL result = url;
        if (url.getProtocol().equals("http") || url.getProtocol().equals("ftp")) {
            String ipAddress = inetAddress instanceof Inet6Address
                    ? "[" + inetAddress.getHostAddress() + "]"
                    : inetAddress.getHostAddress();
            result = substituteHostByIP(url, ipAddress);
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

    private URLConnection checkUrlIncludingHops(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws IOException, URISyntaxException, URLAccessValidationError {
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
            result = checkNotBlockedAndPinToIP(result, securityAuthorizationHandler, securityContext);
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
                        throw new URLAccessValidationError("Redirect limit exceeded");
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

    private static boolean isRedirect(int responseCode) {
        return responseCode >= 300 && responseCode <= 307 && responseCode != 306 && responseCode != HTTP_NOT_MODIFIED;
    }

    public URLConnection validate(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws URLAccessValidationError, IOException {
        String host = url.getHost();
        if (host != null && !host.isEmpty()) {
            try {
                return checkUrlIncludingHops(url, securityAuthorizationHandler, securityContext);
            } catch (URISyntaxException e) {
                throw new URLAccessValidationError("Unable to verify access to " + host + ". Cause: " + e.getMessage());
            }
        } else {
            throw new URLAccessValidationError("Unable to verify access to URL" + url + ". URL is missing a host.");
        }
    }

    @Override
    public CharReadable getReader(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws URLAccessValidationError {
        try {
            InputStream stream = openStream(url, securityAuthorizationHandler, securityContext);
            return Readables.wrap(
                    stream, url.toString(), StandardCharsets.UTF_8, 0); /*length doesn't matter in this context*/
        } catch (IOException | URISyntaxException e) {
            throw new LoadExternalResourceException(
                    String.format("Couldn't load the external resource at: %s", url), e);
        }
    }

    private InputStream openStream(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws IOException, URISyntaxException, URLAccessValidationError {

        URLConnection con = validate(url, securityAuthorizationHandler, securityContext);

        if (con instanceof HttpURLConnection
                && WebURLAccessRule.isRedirect(((HttpURLConnection) con).getResponseCode())) {
            /*
             * Note, HttpURLConnection will stop following a redirect if protocol changes or if Location header is missing
             * (in the current implementation of my java version).
             * WebURLAccessRule.checkUrlIncludingHops will currently also stop if protocol changes,
             * but throws an exception if Location is missing.
             * The http spec recommends to always have a Location header for redirects, but do not strictly forbid it.
             *
             * To be consistent with checkUrlIncludingHops we throw an exception here if we end up at a redirect
             * that can't be followed.
             * This is in line with the recommendations of the spec.
             * If it turns out there is some wretched http server out there that we need to support,
             * that don't respect the spec recommendations, please don't forget to align checkUrlIncludingHops.
             */
            throw new LoadExternalResourceException(String.format(
                    "LOAD CSV failed to access resource. The request to %s was at some point redirected to from which it could not proceed. This may happen if %s redirects to a resource which uses a different protocol than the original request.",
                    con.getURL(), con.getURL()));
        }

        con.setConnectTimeout(CONNECTION_TIMEOUT);
        con.setReadTimeout(READ_TIMOUT);

        var stream = con.getInputStream();
        if ("gzip".equals(con.getContentEncoding())) {
            return new GZIPInputStream(stream);
        } else if ("deflate".equals(con.getContentEncoding())) {
            return new InflaterInputStream(stream);
        } else {
            return stream;
        }
    }
}
