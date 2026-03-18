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

import static org.neo4j.server.web.StaticContent.classpathStaticContent;
import static org.neo4j.server.web.StaticContent.jarStaticContent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Optional;
import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.server.web.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jBrowserModule implements ServerModule {
    private static final String DEFAULT_NEO4J_BROWSER_PATH = "/browser";
    private static final String DEFAULT_NEO4J_BROWSER_STATIC_WEB_CONTENT_LOCATION = "browser";
    private static final Logger log = LoggerFactory.getLogger(Neo4jBrowserModule.class);

    private final WebServer webServer;
    private final Path webDir;
    private final GlobbingPattern browserArtifactPattern;

    public Neo4jBrowserModule(WebServer webServer, Path webDir, GlobbingPattern browserArtifactPattern) {
        this.webServer = webServer;
        this.webDir = webDir;
        this.browserArtifactPattern = browserArtifactPattern;
    }

    @Override
    public void start() {
        if (!Files.exists(webDir)) {
            loadclasspathBrowser();
            return;
        }

        // Fallback to original loading mechanism if zip not found
        try (var webDirListing = Files.list(webDir)) {
            var matchingFiles = webDirListing
                    .filter(path ->
                            browserArtifactPattern.matches(path.getFileName().toString()))
                    .toList();

            if (!matchingFiles.isEmpty()) {
                var sortedList = matchingFiles.stream()
                        .map(file -> {
                            try {
                                return Optional.of(BrowserVersion.fromPath(file));
                            } catch (ParseException e) {
                                log.warn(
                                        "Was not able to parse browser package name: "
                                                + file.getFileName().toString(),
                                        e);
                                return Optional.empty();
                            }
                        })
                        .flatMap(Optional::stream)
                        .sorted()
                        .toList();

                var browserFile = (BrowserVersion) sortedList.getLast();

                if (matchingFiles.size() > 1) {
                    log.warn("Multiple matching browser files found. Loading " + browserFile);
                }

                webServer.addStaticContent(
                        jarStaticContent(browserFile.filePath().toString()), DEFAULT_NEO4J_BROWSER_PATH);
            } else {
                loadclasspathBrowser();
            }
        } catch (IOException e) {
            log.warn("Unable to list files in " + webDir + " for serving browser static content.", e);
        }
    }

    @Override
    public void stop() {
        webServer.removeStaticContent(DEFAULT_NEO4J_BROWSER_PATH);
    }

    private void loadclasspathBrowser() {
        webServer.addStaticContent(
                classpathStaticContent(DEFAULT_NEO4J_BROWSER_STATIC_WEB_CONTENT_LOCATION), DEFAULT_NEO4J_BROWSER_PATH);
    }
}
