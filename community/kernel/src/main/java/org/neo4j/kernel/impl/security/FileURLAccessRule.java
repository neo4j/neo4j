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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.Readables;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * Validating URLs is complex due to the existence of many different types of attacks. Much of the current behaviour,
 * as well as the scenarios we are trying to mitigate, are documented in FileUrlAccessRuleTest.class.
 */
public class FileURLAccessRule implements URLAccessRule {

    private final Configuration config;

    public FileURLAccessRule(Configuration config) {

        this.config = config;
    }

    public URL validate(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws URLAccessValidationError {
        if (!(url.getAuthority() == null || url.getAuthority().equals(""))) {
            throw new URLAccessValidationError(
                    "file URL may not contain an authority section (i.e. it should be 'file:///')");
        }

        if (!(url.getQuery() == null || url.getQuery().equals(""))) {
            throw new URLAccessValidationError("file URL may not contain a query component");
        }

        if (!config.get(GraphDatabaseSettings.allow_file_urls)) {
            throw new URLAccessValidationError(
                    "configuration property '" + GraphDatabaseSettings.allow_file_urls.name() + "' is false");
        }

        try {
            URI result = normalizeURL(url);
            securityAuthorizationHandler.assertLoadAllowed(securityContext, result, null);
            return result.toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private URI normalizeURL(URL url) throws URISyntaxException, URLAccessValidationError {
        if (!((Config) config).isExplicitlySet(GraphDatabaseSettings.load_csv_file_url_root)) {
            return url.toURI();
        } else {
            final Path root = config.get(GraphDatabaseSettings.load_csv_file_url_root);

            // normalize to prevent path traversal exploits like '../'
            final Path urlPath = Path.of(url.toURI().normalize());
            final Path rootPath = root.normalize().toAbsolutePath();
            final Path result = rootPath.resolve(urlPath.getRoot().relativize(urlPath))
                    .normalize()
                    .toAbsolutePath();

            if (result.startsWith(rootPath)) {
                return result.toUri();
            }
            // unreachable because we always construct a path relative to the root
            throw new URLAccessValidationError("file URL points outside configured import directory");
        }
    }

    @Override
    public CharReadable getReader(
            URL url, SecurityAuthorizationHandler securityAuthorizationHandler, SecurityContext securityContext)
            throws URLAccessValidationError, IOException {
        try {
            URL validatedURL = validate(url, securityAuthorizationHandler, securityContext);
            return Readables.files(StandardCharsets.UTF_8, Paths.get(validatedURL.toURI()));
        } catch (URISyntaxException e) {
            throw new URLAccessValidationError("file URL is invalid " + e.getMessage(), e);
        }
    }
}
