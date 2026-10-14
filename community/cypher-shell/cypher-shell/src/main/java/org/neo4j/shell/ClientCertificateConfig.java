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
package org.neo4j.shell;

import java.io.File;
import java.util.Optional;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.ClientCertificateManagers;
import org.neo4j.driver.ClientCertificates;

/**
 * Client certificate used for mutual TLS (mTLS) authentication.
 *
 * @param certificate PEM encoded client certificate (chain)
 * @param privateKey PEM encoded private key for the certificate
 * @param password optional password for an encrypted private key
 */
public record ClientCertificateConfig(File certificate, File privateKey, Optional<String> password) {

    public ClientCertificateManager toClientCertificateManager() {
        var clientCertificate = password.map(pwd -> ClientCertificates.of(certificate, privateKey, pwd))
                .orElseGet(() -> ClientCertificates.of(certificate, privateKey));
        return ClientCertificateManagers.rotating(clientCertificate);
    }
}
