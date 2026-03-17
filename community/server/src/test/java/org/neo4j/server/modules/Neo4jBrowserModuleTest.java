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
package org.neo4j.server.modules;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.server.web.StaticContent;
import org.neo4j.server.web.WebServer;

class Neo4jBrowserModuleTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldStartWithValidZip() throws Exception {
        var webServer = mock(WebServer.class);
        var webDir = tempDir.resolve("web");
        Files.createDirectories(webDir);
        var browserZipPath = webDir.resolve("neo4j-browser.zip");
        Files.writeString(browserZipPath, "irrelevant");

        var module = new Neo4jBrowserModule(
                webServer, webDir, GlobbingPattern.create("neo4j-browser*.zip").getFirst());

        module.start();

        verify(webServer)
                .addStaticContent(eq(StaticContent.jarStaticContent(browserZipPath.toString())), eq("/browser"));
    }

    @Test
    void startShouldFallbackToClasspathWhenNoMatchingFiles() throws Exception {
        var webServer = mock(WebServer.class);
        var webDir = tempDir.resolve("web");
        Files.createDirectories(webDir);
        Files.writeString(webDir.resolve("not-a-browser.zip"), "irrelevant");

        var module = new Neo4jBrowserModule(
                webServer, webDir, GlobbingPattern.create("neo4j-browser*.zip").getFirst());
        module.start();

        verify(webServer).addStaticContent(eq(StaticContent.classpathStaticContent("browser")), eq("/browser"));
    }

    @Test
    void startShouldFallbackToClasspathWhenWebDirDoesNotExist() {
        var webServer = mock(WebServer.class);
        var missingWebDir = tempDir.resolve("does-not-exist");

        var module = new Neo4jBrowserModule(
                webServer,
                missingWebDir,
                GlobbingPattern.create("neo4j-browser*.zip").getFirst());
        module.start();

        verify(webServer).addStaticContent(eq(StaticContent.classpathStaticContent("browser")), eq("/browser"));
    }

    @Test
    void stopShouldRemoveBrowserMountPoint() {
        var webServer = mock(WebServer.class);
        var module = new Neo4jBrowserModule(
                webServer, tempDir, GlobbingPattern.create("neo4j-browser*.zip").getFirst());

        module.stop();

        verify(webServer).removeStaticContent("/browser");
    }
}
