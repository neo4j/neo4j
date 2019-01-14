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
package org.neo4j.kernel.impl.newapi;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.KernelAPIReadTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

class ReadTestSupport implements KernelAPIReadTestSupport
{
    private final Map<Setting,String> settings = new HashMap<>();
    private GraphDatabaseService db;

    void addSetting( Setting setting, String value )
    {
        settings.put( setting, value );
    }

    @Override
    public void setup( File storeDir, Consumer<GraphDatabaseService> create )
    {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder( storeDir );
        settings.forEach( graphDatabaseBuilder::setConfig );
        db = graphDatabaseBuilder.newGraphDatabase();
        create.accept( db );
    }

    @Override
    public Kernel kernelToTest()
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) this.db).getDependencyResolver();
        return resolver.resolveDependency( Kernel.class );
    }

    @Override
    public void tearDown()
    {
        db.shutdown();
        db = null;
    }
}
