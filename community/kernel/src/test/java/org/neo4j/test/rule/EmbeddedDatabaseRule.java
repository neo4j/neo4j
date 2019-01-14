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
package org.neo4j.test.rule;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;


/**
 * JUnit @Rule for configuring, creating and managing an EmbeddedGraphDatabase instance.
 * <p>
 * The database instance is created lazily, so configurations can be injected prior to calling
 * {@link #getGraphDatabaseAPI()}.
 */
public class EmbeddedDatabaseRule extends DatabaseRule
{
    private final TestDirectory testDirectory;

    public EmbeddedDatabaseRule()
    {
        this.testDirectory = TestDirectory.testDirectory();
    }

    @Override
    public EmbeddedDatabaseRule startLazily()
    {
        return (EmbeddedDatabaseRule) super.startLazily();
    }

    @Override
    public File getStoreDir()
    {
        return testDirectory.graphDbDir();
    }

    @Override
    public String getStoreDirAbsolutePath()
    {
        return testDirectory.graphDbDir().getAbsolutePath();
    }

    @Override
    protected GraphDatabaseFactory newFactory()
    {
        return new TestGraphDatabaseFactory();
    }

    @Override
    protected GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory )
    {
        return factory.newEmbeddedDatabaseBuilder( getStoreDir() );
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return testDirectory.apply( super.apply( base, description ), description );
    }

    /**
     * Get the inner {@link TestDirectory} instance that is used to prepare the store directory for this database.
     * <p>
     * <strong>Note:</strong> There is no need to add a {@link org.junit.Rule} annotation on this {@link TestDirectory}
     * instance.
     */
    public TestDirectory getTestDirectory()
    {
        return testDirectory;
    }
}
