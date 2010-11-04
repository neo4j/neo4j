/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.webadmin;

import java.io.File;

import org.neo4j.rest.domain.DatabaseLocator;

/**
 * This is here temporarily, it is simply a copy-paste of a class in the REST
 * test suite.
 * 
 */
public abstract class TestUtil
{ 
    /**
     * The "admin" part is here temporarily, while we wait to move management
     * services into the REST project instead.
     */
    public static String SERVER_BASE = AdminServer.INSTANCE.getBaseUri()
                                       + "manage";

    public static void deleteTestDb()
    {
        deleteFileOrDirectory( new File( DatabaseLocator.getDatabaseLocation() ) );
    }

    public static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }

}
