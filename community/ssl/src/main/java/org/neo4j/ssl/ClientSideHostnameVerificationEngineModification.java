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

import java.util.function.Function;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * Client side modifier for SSLEngine to mandate hostname verification
 */
public class ClientSideHostnameVerificationEngineModification implements Function<SSLEngine,SSLEngine>
{
    /**
     * Apply modifications to engine to enable hostname verification (client side only)
     *
     * @param sslEngine the engine used for handling TLS. Will be mutated by this method
     * @return the updated sslEngine that allows client side hostname verification
     */
    @Override
    public SSLEngine apply( SSLEngine sslEngine )
    {
        SSLParameters sslParameters = sslEngine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm( VerificationAlgorithm.HTTPS.getValue() );
        sslEngine.setSSLParameters( sslParameters );
        return sslEngine;
    }

    private enum VerificationAlgorithm
    {
        /*
        Endpoint identification algorithms
        HTTPS http://www.ietf.org/rfc/rfc2818.txt
        LDAPS http://www.ietf.org/rfc/rfc2830.txt
         */
        HTTPS( "HTTPS" ),
        LDAPS( "LDAPS" );

        private final String value;

        VerificationAlgorithm( String value )
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }

}
