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
package org.neo4j.kernel.impl.security;

import java.net.URL;
import java.util.Map;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.security.URLAccessValidationError;

public class URLAccessRules
{
    private static final URLAccessRule ALWAYS_PERMITTED = new URLAccessRule()
    {
        @Override
        public URL validate( GraphDatabaseAPI gdb, URL url )
        {
            return url;
        }
    };

    public static URLAccessRule alwaysPermitted()
    {
        return ALWAYS_PERMITTED;
    }

    private static final URLAccessRule FILE_ACCESS = new FileURLAccessRule();

    public static URLAccessRule fileAccess()
    {
        return FILE_ACCESS;
    }

    public static URLAccessRule combined( final Map<String,URLAccessRule> urlAccessRules )
    {
        return new URLAccessRule()
        {
            @Override
            public URL validate( GraphDatabaseAPI gdb, URL url ) throws URLAccessValidationError
            {
                String protocol = url.getProtocol();
                URLAccessRule protocolRule = urlAccessRules.get( protocol );
                if ( protocolRule == null )
                {
                    throw new URLAccessValidationError( "loading resources via protocol '" + protocol + "' is not permitted" );
                }
                return protocolRule.validate( gdb, url );
            }
        };
    }
}
