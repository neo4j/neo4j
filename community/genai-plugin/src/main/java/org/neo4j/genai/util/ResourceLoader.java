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
package org.neo4j.genai.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.security.FileURIAccessRule;

public final class ResourceLoader {
    public static final Set<String> WEB_PROTOCOLS = Set.of("http", "https", "ftp");
    private static final int MAX_REDIRECTS = 10;

    private ResourceLoader() {}

    /**
     * Returns true if the string has a recognized URI scheme for file loading:
     * {@code file}, {@code http}, {@code https}, or {@code ftp}.
     * Base64 strings can never match because {@code :} is not in the base64 alphabet.
     */
    public static boolean isFileReference(String resource) {
        try {
            URI uri = new URI(resource);
            String scheme = uri.getScheme();
            if (scheme == null) {
                return false;
            }
            String schemeLower = scheme.toLowerCase(Locale.ROOT);
            return "file".equals(schemeLower) || WEB_PROTOCOLS.contains(schemeLower);
        } catch (URISyntaxException e) {
            return false;
        }
    }

    /**
     * Opens the given file or URL reference as an {@link InputStream}.
     * Applies the same security checks as {@code LOAD CSV} for local files,
     * and {@link URLAccessChecker} for remote resources.
     */
    public static InputStream openResource(
            String file, URLAccessChecker urlAccessChecker, GenAIConfig genAIConfig, SecurityContext securityContext)
            throws IOException, URLAccessValidationError {
        URI uri;
        try {
            uri = new URI(file);
            if (uri.getScheme() == null) {
                throw new URLAccessValidationError(
                        "Missing protocol: `" + file + "`. Files must be prepended with `file:///");
            }
        } catch (URISyntaxException e) {
            uri = Path.of(file).toUri();
        }

        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if ("file".equals(scheme)) {
            return openLocalFile(uri, genAIConfig, securityContext);
        } else if (WEB_PROTOCOLS.contains(scheme)) {
            return openRemoteFile(uri.toURL(), urlAccessChecker);
        } else {
            throw new IllegalArgumentException("Unsupported protocol: " + scheme);
        }
    }

    private static InputStream openLocalFile(URI uri, GenAIConfig genAIConfig, SecurityContext securityContext)
            throws IOException, URLAccessValidationError {
        var rule = new FileURIAccessRule(genAIConfig.getNeo4jConfig());
        var handler = new SecurityAuthorizationHandler(CommunitySecurityLog.NULL_LOG);
        URI validated = rule.validate(uri, handler, securityContext);
        return Files.newInputStream(Path.of(validated));
    }

    private static InputStream openRemoteFile(URL url, URLAccessChecker urlAccessChecker)
            throws IOException, URLAccessValidationError {
        URL checked = urlAccessChecker.checkURL(url);
        return openWithRedirects(checked, 0, urlAccessChecker).getInputStream();
    }

    private static URLConnection openWithRedirects(URL url, int redirectCount, URLAccessChecker urlAccessChecker)
            throws IOException, URLAccessValidationError {
        if (redirectCount > MAX_REDIRECTS) {
            throw new IOException("Too many redirects");
        }

        URLConnection connection = url.openConnection();
        if (connection instanceof HttpURLConnection httpConnection) {
            httpConnection.setInstanceFollowRedirects(false);
            int status = httpConnection.getResponseCode();
            if (status == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new FileNotFoundException(url.toString());
            }
            if (status >= 300 && status <= 307 && status != 306) {
                String location = httpConnection.getHeaderField("Location");
                if (location != null) {
                    URL newUrl = new URL(url, location);
                    if (!url.getProtocol().equalsIgnoreCase(newUrl.getProtocol())) {
                        throw new SecurityException(
                                "Protocol change during redirect is not allowed for security reasons.");
                    }
                    URL checkedUrl = urlAccessChecker.checkURL(newUrl);
                    return openWithRedirects(checkedUrl, redirectCount + 1, urlAccessChecker);
                }
            }
        }

        return connection;
    }
}
