/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Interacts with the environment in a way that abstracts away the specifics of the current operating system.
 */
public interface Environment
{
    void openBrowser( String url ) throws IOException, URISyntaxException;

    void editFile( File file ) throws IOException;

    void openDirectory( File directory ) throws IOException;

    void openCommandPrompt( File binDirectory, File jreBinDirectory, File workingDirectory ) throws IOException;
}
