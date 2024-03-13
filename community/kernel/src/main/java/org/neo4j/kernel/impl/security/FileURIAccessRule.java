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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.function.Supplier;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.Readables;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

/**
 * Validating URIs is complex due to the existence of many different types of attacks. Much of the current behaviour,
 * as well as the scenarios we are trying to mitigate, are documented in FileURIAccessRuleTest.class.
 */
public class FileURIAccessRule implements AccessRule<URI> {

    private final Supplier<SchemeFileSystemAbstraction> schemeSystemSupplier;
    private final Config config;

    public FileURIAccessRule(Config config) {
        this(() -> new SchemeFileSystemAbstraction(new DefaultFileSystemAbstraction(), config), config);
    }

    public FileURIAccessRule(Supplier<SchemeFileSystemAbstraction> schemeSystemSupplier, Config config) {
        this.schemeSystemSupplier = schemeSystemSupplier;
        this.config = config;
    }

    public URI validate(
            URI uri, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws URLAccessValidationError {
        final var fileLikeScheme = isFileLikeScheme(uri);
        if (fileLikeScheme && !(uri.getAuthority() == null || uri.getAuthority().isEmpty())) {
            throw new URLAccessValidationError(
                    "file URL may not contain an authority section (i.e. it should be 'file:///')");
        }

        if (!(uri.getQuery() == null || uri.getQuery().isEmpty())) {
            throw new URLAccessValidationError("file URL may not contain a query component");
        }

        if (!config.get(GraphDatabaseSettings.allow_file_urls)) {
            throw new URLAccessValidationError(
                    "configuration property '" + GraphDatabaseSettings.allow_file_urls.name() + "' is false");
        }

        try {
            URI result = fileLikeScheme ? normalizeURI(uri) : uri.normalize();
            securityAuthorizationHandler.assertLoadAllowed(securityContext, result, null);
            return result;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CharReadable getReader(
            URI uri, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws URLAccessValidationError, IOException {
        final var scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        try (var fs = schemeSystemSupplier.get()) {
            if (!fs.resolvableSchemes().contains(scheme)) {
                throw new URLAccessValidationError("Invalid URL '" + uri + "': unknown protocol: " + scheme);
            }

            if (!fs.canResolve(uri)) {
                throw new URLAccessValidationError("loading resources via protocol '" + scheme + "' is not permitted");
            }

            final var path = fs.resolve(validate(uri, securityAuthorizationHandler, securityContext));
            return Readables.files(StandardCharsets.UTF_8, path);
        }
    }

    private boolean isFileLikeScheme(URI uri) {
        final var scheme = uri.getScheme();
        return scheme == null || "file".equalsIgnoreCase(scheme);
    }

    private URI normalizeURI(URI uri) throws URISyntaxException, URLAccessValidationError {
        if (!config.isExplicitlySet(GraphDatabaseSettings.load_csv_file_url_root)) {
            return uri;
        }

        final Path root = config.get(GraphDatabaseSettings.load_csv_file_url_root);
        // normalize to prevent path traversal exploits like '../'
        final Path uriPath = Path.of(uri.normalize());
        final Path rootPath = root.normalize().toAbsolutePath();
        final Path result = rootPath.resolve(uriPath.getRoot().relativize(uriPath))
                .normalize()
                .toAbsolutePath();

        if (result.startsWith(rootPath)) {
            return result.toUri();
        }

        // unreachable because we always construct a path relative to the root
        throw new URLAccessValidationError("file URL points outside configured import directory");
    }
}
