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
import java.security.KeyStore;

public class KeyStoreInformation
{
    private final char[] keyStorePassword;
    private final char[] keyPassword;
    private final File privateKeyPath;
    private final File certificatePath;
    private final KeyStore keyStore;

    public KeyStoreInformation( KeyStore keyStore, char[] keyStorePassword, char[] keyPassword, File privateKeyPath,
            File certificatePath )
    {
        this.keyStore = keyStore;
        this.keyStorePassword = keyStorePassword;
        this.keyPassword = keyPassword;
        this.privateKeyPath = privateKeyPath;
        this.certificatePath = certificatePath;
    }

    public char[] getKeyStorePassword()
    {
        return keyStorePassword;
    }

    public char[] getKeyPassword()
    {
        return keyPassword;
    }

    public KeyStore getKeyStore()
    {
        return keyStore;
    }

    public File getPrivateKeyPath()
    {
        return privateKeyPath;
    }

    public File getCertificatePath()
    {
        return certificatePath;
    }
}
