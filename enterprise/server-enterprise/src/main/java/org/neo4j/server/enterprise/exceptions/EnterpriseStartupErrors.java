/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.enterprise.exceptions;

import org.neo4j.causalclustering.identity.BootstrapConnectionTimeout;
import org.neo4j.helpers.Exceptions;
import org.neo4j.server.ServerStartupException;
import org.neo4j.server.exception.CommunityStartupErrors;
import org.neo4j.server.exception.WellKnownStartupException;

/**
 * Enterprise-specific startup errors, see {@link CommunityStartupErrors}.
 */
public class EnterpriseStartupErrors
{
    private EnterpriseStartupErrors()
    {

    }

    public static ServerStartupException translateEnterpriseStartupError( Throwable cause )
    {
        Throwable rootCause = Exceptions.rootCause( cause );

        if ( rootCause instanceof BootstrapConnectionTimeout )
        {
            return new WellKnownStartupException( rootCause );
        }

        // No idea, fall back to see if community edition knows
        return CommunityStartupErrors.translateCommunityStartupError( cause );
    }
}