/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.embedded;

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;

/**
 * A running Neo4j graph database, with additional methods exposed for test purposes.
 */
public interface TestGraphDatabase extends GraphDatabase
{
    abstract class Builder extends TestGraphDatabaseBuilder<Builder>
    {
    }

    abstract class EphemeralBuilder extends TestGraphDatabaseBuilder<EphemeralBuilder>
    {
        public abstract TestGraphDatabase open();
    }

    FileSystemAbstraction fileSystem();

    File storeDir();

    DependencyResolver getDependencyResolver();

    /**
     * @deprecated Method included for transitional purpose only - prefer not using this method
     */
    @Deprecated
    GraphDatabaseAPI getGraphDatabaseAPI();
}
