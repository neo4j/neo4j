/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

public class TemporalIndexPopulationStressTest extends IndexPopulationStressTest
{
    @Override
    IndexProvider newProvider( IndexDirectoryStructure.Factory directory )
    {
        return new TemporalIndexProvider( rules.pageCache(),
                                          rules.fileSystem(),
                                          directory,
                                          IndexProvider.Monitor.EMPTY,
                                          RecoveryCleanupWorkCollector.IMMEDIATE,
                                          false );
    }

    @Override
    Value randomValue( RandomValues random )
    {
        return random.nextTemporalValue();
    }
}
