/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.SystemNanoClock;

public class TestGraphDatabaseFactoryState extends GraphDatabaseFactoryState
{
    private FileSystemAbstraction fileSystem;
    private LogProvider internalLogProvider;
    private SystemNanoClock clock;

    public TestGraphDatabaseFactoryState()
    {
        fileSystem = null;
        internalLogProvider = null;
    }

    public TestGraphDatabaseFactoryState( TestGraphDatabaseFactoryState previous )
    {
        super( previous );
        fileSystem = previous.fileSystem;
        internalLogProvider = previous.internalLogProvider;
        clock = previous.clock;
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    public void setFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public LogProvider getInternalLogProvider()
    {
        return internalLogProvider;
    }

    public void setInternalLogProvider( LogProvider logProvider )
    {
        this.internalLogProvider = logProvider;
    }

    public SystemNanoClock clock()
    {
        return clock;
    }

    public void setClock( SystemNanoClock clock )
    {
        this.clock = clock;
    }
}
