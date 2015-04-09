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
package org.neo4j.driver.internal.connector.http;

import java.net.URI;
import java.util.Collection;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.Logging;

import static java.util.Arrays.asList;

public class HttpConnector implements Connector
{
    public static final String USER_AGENT = "Neo4j-Java/2.2";
    public static final String SCHEME = "neo4j+http";
    public static final int DEFAULT_PORT = 7687;

    private Logging logging;

    @Override
    public boolean supports( String scheme )
    {
        return scheme.equals( SCHEME );
    }

    @Override
    public Connection connect( URI sessionURL ) throws ClientException
    {
        return new HttpConnection( sessionURL, logging.getLogging( "neo4j.transport" ), DEFAULT_PORT );
    }

    @Override
    public void setLogging( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public Collection<String> supportedSchemes()
    {
        return asList( SCHEME );
    }
}
