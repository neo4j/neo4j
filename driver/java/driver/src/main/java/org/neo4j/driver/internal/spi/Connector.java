/**
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
package org.neo4j.driver.internal.spi;

import java.net.URI;
import java.util.Collection;

import org.neo4j.driver.exceptions.ClientException;

/**
 * A Connector conducts the client side of a client-server dialogue,
 * along with its server side counterpart, the Listener.
 */
public interface Connector
{
    /**
     * Determine whether this connector can support the sessionURL specified.
     *
     * @param scheme a URL scheme
     * @return true if this scheme is supported, false otherwise
     */
    boolean supports( String scheme );

    /**
     * Establish a connection to a remote listener and attach to the session identified.
     *
     * @param sessionURL a URL identifying a remote session
     * @return a Connection object
     */
    Connection connect( URI sessionURL ) throws ClientException;

    /**
     * Set the logging to be used by this connector. All connections created after this call will use the provided
     * logging instance.
     */
    void setLogging( Logging logging );

    /** List names of supported schemes, used for error messages and similar signaling to end users. */
    Collection<String> supportedSchemes();
}
