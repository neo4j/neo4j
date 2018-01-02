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
package org.neo4j.test;

import org.junit.rules.ExternalResource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

public class DefaultFileSystemRule extends ExternalResource
{
    private DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Override
    protected void after()
    {

    }

    public DefaultFileSystemAbstraction get()
    {
        return fs;
    }

    public DefaultFileSystemAbstraction snapshot( Runnable action )
    {
        DefaultFileSystemAbstraction snapshot = fs;
        try
        {
            action.run();
        }
        finally
        {
            fs = snapshot;
        }
        return fs;
    }
    
    public void clear()
    {
        fs = new DefaultFileSystemAbstraction();
    }

    public static Runnable shutdownDb( final GraphDatabaseService db )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                db.shutdown();
            }
        };
    }
}
