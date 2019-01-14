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

public class EssentialEngineModifications implements Function<SSLEngine,SSLEngine>
{
    private final String[] tlsVersions;
    private final boolean isClient;

    public EssentialEngineModifications( String[] tlsVersions, boolean isClient )
    {
        this.tlsVersions = tlsVersions;
        this.isClient = isClient;
    }

    /**
     * Apply engine modifications that will exist in any use-case of TLS
     *
     * @param sslEngine the ssl engine that will be used for the connections. Is mutated.
     * @return the updated sslEngine (should be the same as the original, but don't rely on that)
     */
    @Override
    public SSLEngine apply( SSLEngine sslEngine )
    {
        if ( tlsVersions != null )
        {
            sslEngine.setEnabledProtocols( tlsVersions );
        }
        sslEngine.setUseClientMode( isClient );
        return sslEngine;
    }
}
