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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.security.FileURIAccessRule;

public final class ResourceLoader {
    public static final Set<String> WEB_PROTOCOLS = Set.of("http", "https", "ftp");
    private static final int MAX_REDIRECTS = 10;
    private static final long MAX_DECOMPRESSED_SIZE = ByteUnit.mebiBytes(100);

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
                throw new GenAIProcedureException(
                        "Missing protocol: `" + file + "`. Files must be prepended with `file:///");
            }
        } catch (URISyntaxException e) {
            uri = Path.of(file).toUri();
        }

        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        InputStream rawStream;
        if ("file".equals(scheme)) {
            rawStream = openLocalFile(uri, genAIConfig, securityContext);
        } else if (WEB_PROTOCOLS.contains(scheme)) {
            rawStream = openRemoteFile(uri.toURL(), urlAccessChecker);
        } else {
            throw new GenAIProcedureException("Unsupported protocol: " + scheme);
        }
        return decompressIfNeeded(rawStream, file);
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
            throw new GenAIProcedureException("Too many redirects");
        }

        URLConnection connection = url.openConnection();
        if (connection instanceof HttpURLConnection httpConnection) {
            httpConnection.setInstanceFollowRedirects(false);
            int status = httpConnection.getResponseCode();
            if (status == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new GenAIProcedureException("File not found: " + url);
            }
            if (status >= 300 && status <= 307 && status != 306) {
                String location = httpConnection.getHeaderField("Location");
                if (location != null) {
                    URL newUrl = new URL(url, location);
                    if (!url.getProtocol().equalsIgnoreCase(newUrl.getProtocol())) {
                        throw new GenAIProcedureException(
                                "Protocol change during redirect is not allowed for security reasons.");
                    }
                    URL checkedUrl = urlAccessChecker.checkURL(newUrl);
                    return openWithRedirects(checkedUrl, redirectCount + 1, urlAccessChecker);
                }
            }
        }

        return connection;
    }

    private static InputStream decompressIfNeeded(InputStream inputStream, String source) throws IOException {
        // Read enough bytes to detect ZIP (4 bytes) or GZIP (2 bytes) magic numbers.
        byte[] magic = new byte[4];
        var pushback = new PushbackInputStream(inputStream, magic.length);
        int read = pushback.read(magic);
        if (read < 0) {
            return pushback;
        }
        byte[] header = read < magic.length ? Arrays.copyOf(magic, read) : magic;

        if (isGzip(header)) {
            pushback.unread(header, 0, read);
            return new BoundedInputStream(new GZIPInputStream(pushback), MAX_DECOMPRESSED_SIZE, source);
        } else if (isZip(header)) {
            pushback.unread(header, 0, read);
            return extractZipEntry(pushback, source);
        }

        pushback.unread(header, 0, read);
        return pushback;
    }

    private static boolean isGzip(byte[] bytes) {
        return bytes.length >= 2 && (bytes[0] & 0xFF) == 0x1f && (bytes[1] & 0xFF) == 0x8b;
    }

    private static boolean isZip(byte[] bytes) {
        return bytes.length >= 4
                && (bytes[0] & 0xFF) == 0x50 // P
                && (bytes[1] & 0xFF) == 0x4b // K
                && (bytes[2] & 0xFF) == 0x03
                && (bytes[3] & 0xFF) == 0x04;
    }

    private static InputStream extractZipEntry(InputStream input, String source) throws IOException {
        try (var zipStream = new ZipInputStream(input)) {
            ZipEntry entry;
            byte[] suitableContent = null;
            while ((entry = zipStream.getNextEntry()) != null) {
                if (!entry.isDirectory() && !isInvalidZipEntry(entry.getName())) {
                    if (suitableContent != null) {
                        throw new GenAIProcedureException("ZIP archive contains more than one file: " + source);
                    }
                    suitableContent = readAllBytesWithLimit(zipStream, MAX_DECOMPRESSED_SIZE, source);
                }
            }

            if (suitableContent != null) {
                return new ByteArrayInputStream(suitableContent);
            }
        }
        throw new GenAIProcedureException("No suitable file found in ZIP archive: " + source);
    }

    private static byte[] readAllBytesWithLimit(InputStream in, long limit, String source) throws IOException {
        var baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        long totalRead = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            totalRead += read;
            if (totalRead > limit) {
                throw new GenAIProcedureException(
                        "Decompressed file size exceeds the limit of " + ByteUnit.bytesToString(limit) + ": " + source);
            }
            baos.write(buffer, 0, read);
        }
        return baos.toByteArray();
    }

    private static class BoundedInputStream extends InputStream {
        private final InputStream in;
        private final long limit;
        private final String source;
        private long totalRead;

        BoundedInputStream(InputStream in, long limit, String source) {
            this.in = in;
            this.limit = limit;
            this.source = source;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b != -1) {
                incrementBytesRead(1);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = in.read(b, off, len);
            if (read != -1) {
                incrementBytesRead(read);
            }
            return read;
        }

        private void incrementBytesRead(int read) {
            totalRead += read;
            if (totalRead > limit) {
                throw new GenAIProcedureException(
                        "Decompressed file size exceeds the limit of " + ByteUnit.bytesToString(limit) + ": " + source);
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    private static boolean isInvalidZipEntry(String name) {
        return name.contains("__MACOSX") || name.startsWith(".") || name.contains("/.");
    }
}
