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
package org.neo4j.ssl;

import java.nio.file.Path;

public class SslResource {
    private final Path privateKey;
    private final Path publicCertificate;
    private final Path trustedDirectory;
    private final Path revokedDirectory;

    SslResource(Path privateKey, Path publicCertificate, Path trustedDirectory, Path revokedDirectory) {
        this.privateKey = privateKey;
        this.publicCertificate = publicCertificate;
        this.trustedDirectory = trustedDirectory;
        this.revokedDirectory = revokedDirectory;
    }

    public Path privateKey() {
        return privateKey;
    }

    public Path publicCertificate() {
        return publicCertificate;
    }

    public Path trustedDirectory() {
        return trustedDirectory;
    }

    public Path revokedDirectory() {
        return revokedDirectory;
    }
}
