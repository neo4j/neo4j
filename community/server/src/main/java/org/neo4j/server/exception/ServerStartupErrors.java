/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.exception;

import org.neo4j.function.Function;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedException;
import org.neo4j.server.ServerStartupException;

import static java.lang.String.format;

/**
 * Helps translate known common user errors to avoid long java stack traces and other bulk logging that obscures
 * what went wrong.
 */
public class ServerStartupErrors
{
    /**
     * Each function in this array handles translating one case. If it doesn't know how to translate a given
     * throwable, it simply returns null.
     */
    private static final Function<Throwable, ServerStartupException>[] translators = new Function[] {
        // Handle upgrade errors
        new Function<Throwable, ServerStartupException>()
        {
            @Override
            public ServerStartupException apply( Throwable o )
            {
                Throwable rootCause = Exceptions.rootCause( o );
                if( rootCause instanceof UpgradeNotAllowedException )
                {
                    return new UpgradeDisallowedStartupException( (UpgradeNotAllowedException)rootCause );
                }
                return null;
            }
        }
    };

    public static ServerStartupException translateToServerStartupError( Throwable cause )
    {
        for ( Function<Throwable,ServerStartupException> translator : translators )
        {
            ServerStartupException r = translator.apply( cause );
            if(r != null)
            {
                return r;
            }
        }

        return new ServerStartupException( format( "Starting Neo4j failed: %s", cause.getMessage() ), cause );
    }

}
