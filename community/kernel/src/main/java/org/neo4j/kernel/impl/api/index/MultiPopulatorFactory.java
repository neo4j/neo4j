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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.logging.LogProvider;

/**
 * Factory that is able to create either {@link MultipleIndexPopulator} or {@link BatchingMultipleIndexPopulator}
 * depending on the given config.
 *
 * @see GraphDatabaseSettings#multi_threaded_schema_index_population_enabled
 */
public abstract class MultiPopulatorFactory
{
    private MultiPopulatorFactory()
    {
    }

    public abstract MultipleIndexPopulator create( IndexStoreView storeView, LogProvider logProvider,
                                                   SchemaState schemaState );

    public static MultiPopulatorFactory forConfig( Config config )
    {
        boolean multiThreaded = config.get( GraphDatabaseSettings.multi_threaded_schema_index_population_enabled );
        return multiThreaded ? new MultiThreadedPopulatorFactory() : new SingleThreadedPopulatorFactory();
    }

    private static class SingleThreadedPopulatorFactory extends MultiPopulatorFactory
    {
        @Override
        public MultipleIndexPopulator create( IndexStoreView storeView, LogProvider logProvider,
                                              SchemaState schemaState )
        {
            return new MultipleIndexPopulator( storeView, logProvider, schemaState );
        }
    }

    private static class MultiThreadedPopulatorFactory extends MultiPopulatorFactory
    {
        @Override
        public MultipleIndexPopulator create( IndexStoreView storeView, LogProvider logProvider,
                                              SchemaState schemaState )
        {
            return new BatchingMultipleIndexPopulator( storeView, logProvider, schemaState );
        }
    }
}
