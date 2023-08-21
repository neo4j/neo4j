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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.security.URLAccessValidationError;

/**
 * Validating URLs is complex due to the existence of many different types of attacks. Much of the current behaviour,
 * as well as the scenarios we are trying to mitigate, are documented in FileUrlAccessRuleTest.class.
 */
class FileURLAccessRule implements URLAccessRule {
    @Override
    public URL validate(Configuration config, URL url) throws URLAccessValidationError {
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

        if (!((Config) config).isExplicitlySet(GraphDatabaseSettings.load_csv_file_url_root)) {
            return url;
        }
        final Path root = config.get(GraphDatabaseSettings.load_csv_file_url_root);

        try {
            // normalize to prevent path traversal exploits like '../'
            final Path urlPath = Path.of(url.toURI().normalize());
            final Path rootPath = root.normalize().toAbsolutePath();
            final Path result = rootPath.resolve(urlPath.getRoot().relativize(urlPath))
                    .normalize()
                    .toAbsolutePath();

            if (result.startsWith(rootPath)) {
                return result.toUri().toURL();
            }
            // unreachable because we always construct a path relative to the root
            throw new URLAccessValidationError("file URL points outside configured import directory");
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
