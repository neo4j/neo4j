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
import java.net.URL;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.LoadExternalResourceException;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.NullLog;

public class URLAccessRulesTest {

    @Test
    public void shouldValidateAndOpenFileUrls() throws Exception {

        File file = File.createTempFile("test", "csv");
        URLAccessRules rules = new URLAccessRules(new CommunitySecurityLog(NullLog.getInstance()), Config.defaults());

        rules.validateAndOpen(
                SecurityContext.AUTH_DISABLED, new URL(String.format("file://%s", file.getAbsolutePath())));
        rules.validateAndOpen(
                SecurityContext.AUTH_DISABLED, new URL(String.format("FILE://%s", file.getAbsolutePath())));

        file.delete();
    }

    @Test
    public void shouldNotReadUnknownScheme() {
        URLAccessRules rules = new URLAccessRules(new CommunitySecurityLog(NullLog.getInstance()), Config.defaults());
        assertThrows(
                URLAccessValidationError.class,
                () -> rules.validateAndOpen(
                        SecurityContext.AUTH_DISABLED,
                        new URL("jar:http://www.foo.com/bar/baz.jar!/COM/foo/test.csv")));
    }

    @Test
    public void shouldNotAllowReadOutsideOfImportDir() throws Exception {

        File file = File.createTempFile("test", "csv");
        URLAccessRules rules = new URLAccessRules(
                new CommunitySecurityLog(NullLog.getInstance()),
                Config.defaults(GraphDatabaseSettings.load_csv_file_url_root, Paths.get("/import")));

        assertThrows(
                LoadExternalResourceException.class,
                () -> rules.validateAndOpen(
                        SecurityContext.AUTH_DISABLED, new URL(String.format("file://%s", file.getAbsolutePath()))));
    }
}
