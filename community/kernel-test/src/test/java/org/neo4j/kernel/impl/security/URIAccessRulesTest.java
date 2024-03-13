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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.LoadExternalResourceException;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.NullLog;

public class URIAccessRulesTest {

    @ParameterizedTest
    @ValueSource(strings = {"file", "FILE", "FiLe"})
    public void shouldValidateAndOpenFileUrls(String fileScheme) throws Exception {
        File file = File.createTempFile("test", "csv");
        file.deleteOnExit();

        URIAccessRules rules = new URIAccessRules(new CommunitySecurityLog(NullLog.getInstance()), Config.defaults());

        // Windows needs triple-slashes...
        String slashes = SystemUtils.IS_OS_WINDOWS ? "///" : "//";

        rules.validateAndOpen(
                SecurityContext.AUTH_DISABLED,
                new URI(String.format("%s:%s%s", fileScheme, slashes, file.getAbsolutePath())));
    }

    @Test
    public void shouldNotReadUnknownScheme() {
        URIAccessRules rules = new URIAccessRules(new CommunitySecurityLog(NullLog.getInstance()), Config.defaults());
        assertThrows(
                URLAccessValidationError.class,
                () -> rules.validateAndOpen(
                        SecurityContext.AUTH_DISABLED,
                        new URI("jar:http://www.foo.com/bar/baz.jar!/COM/foo/test.csv")));
    }

    @Test
    public void shouldNotAllowReadOutsideOfImportDir() throws Exception {
        File file = File.createTempFile("test", "csv");
        file.deleteOnExit();

        URIAccessRules rules = new URIAccessRules(
                new CommunitySecurityLog(NullLog.getInstance()),
                Config.defaults(GraphDatabaseSettings.load_csv_file_url_root, Paths.get("/import")));

        // Windows needs triple-slashes...
        String slashes = SystemUtils.IS_OS_WINDOWS ? "///" : "//";

        assertThrows(
                LoadExternalResourceException.class,
                () -> rules.validateAndOpen(
                        SecurityContext.AUTH_DISABLED,
                        new URI(String.format("file:%s%s", slashes, file.getAbsolutePath()))));
    }
}
