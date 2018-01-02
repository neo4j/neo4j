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

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

public class EphemeralFileSystemRule extends ExternalResource implements Supplier<FileSystemAbstraction>
{
    private EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

    @Override
    protected void after()
    {
        fs.shutdown();
    }

    @Override
    public final EphemeralFileSystemAbstraction get()
    {
        return fs;
    }

    public EphemeralFileSystemAbstraction snapshot( Runnable action )
    {
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        try
        {
            action.run();
        }
        finally
        {
            fs.shutdown();
            fs = snapshot;
        }
        return fs;
    }
    
    public void clear()
    {
        fs.shutdown();
        fs = new EphemeralFileSystemAbstraction();
    }

    public static Runnable shutdownDbAction( final GraphDatabaseService db )
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
