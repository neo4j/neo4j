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
package org.neo4j.bolt.protocol.common.connector.config;

import io.netty.handler.ssl.SslContext;
import org.neo4j.ssl.config.ScopedSslPolicyProvider;

public interface NettyConnectorConfiguration extends ConnectorConfiguration {

    /**
     * Identifies whether this connector shall use the merge cumulator instead of making use of a
     * composite-based cumulator implementation.
     * <p/>
     * This configuration may lead to additional memory consumption as well as performance
     * degradation.
     *
     * @return true if enabled, false otherwise.
     */
    boolean enableMergeCumulator();

    /**
     * Retrieves the SSL context which shall be used to facilitate TLS functionality within the
     * context of this connector.
     * <p>
     * When no TLS is desired, null is returned instead.
     *
     * @return an SSL context or null.
     */
    SslContext sslContext();

    /**
     * Identifies whether this connector shall support the PROXY protocol (HAProxy v1/v2).
     * <p/>
     * When enabled, the connector will automatically detect and decode PROXY protocol headers
     * from load balancers (e.g., HAProxy, AWS NLB) that provide the real client IP address.
     * The connector gracefully handles both connections with and without PROXY protocol headers,
     * making it safe to enable even in mixed environments.
     * <p/>
     * The real client address will be used for authentication, logging, connection tracking,
     * and security auditing instead of the proxy's address.
     *
     * @return true if PROXY protocol support is enabled, false otherwise.
     */
    boolean enableProxyProtocol();

    interface Factory<SELF extends Factory<SELF>> extends ConnectorConfiguration.Factory<SELF> {

        @Override
        NettyConnectorConfiguration build();

        SELF enableMergeCumulator(boolean value);

        SELF sslPolicyProvider(ScopedSslPolicyProvider policyProvider);

        SELF enableProxyProtocol(boolean value);
    }
}
