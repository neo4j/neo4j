/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ssl;

import java.io.File;

public class SslResource
{
    private final File privateKey;
    private final File publicCertificate;
    private final File trustedDirectory;
    private final File revokedDirectory;

    SslResource( File privateKey, File publicCertificate, File trustedDirectory, File revokedDirectory )
    {
        this.privateKey = privateKey;
        this.publicCertificate = publicCertificate;
        this.trustedDirectory = trustedDirectory;
        this.revokedDirectory = revokedDirectory;
    }

    public File privateKey()
    {
        return privateKey;
    }

    public File publicCertificate()
    {
        return publicCertificate;
    }

    public File trustedDirectory()
    {
        return trustedDirectory;
    }

    public File revokedDirectory()
    {
        return revokedDirectory;
    }
}
