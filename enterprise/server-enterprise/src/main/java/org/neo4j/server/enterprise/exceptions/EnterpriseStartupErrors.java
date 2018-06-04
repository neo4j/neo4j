/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
