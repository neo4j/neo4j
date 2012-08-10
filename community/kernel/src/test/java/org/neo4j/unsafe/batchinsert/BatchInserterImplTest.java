/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.test.ReflectionUtil;

public class BatchInserterImplTest
{
    @Test
    public void testHonorsPassedInParams() throws Exception
    {
        Boolean memoryMappingConfig = createInserterAndGetMemoryMappingConfig( stringMap( GraphDatabaseSettings
                .use_memory_mapped_buffers.name(), "true" ) );
        assertTrue( "memory mapped config is active", memoryMappingConfig );
    }

    @Test
    public void testDefaultsToNoMemoryMapping() throws Exception
    {
        Boolean memoryMappingConfig = createInserterAndGetMemoryMappingConfig( stringMap() );
        assertFalse( "memory mapped config is active", memoryMappingConfig );
    }

    private Boolean createInserterAndGetMemoryMappingConfig( Map<String, String> initialConfig ) throws Exception
    {
        BatchInserterImpl inserter = new BatchInserterImpl( "target/batch-inserter-test", initialConfig );
        NeoStore neoStore = ReflectionUtil.getPrivateField( inserter, "neoStore", NeoStore.class );
        Config config = ReflectionUtil.getPrivateField( neoStore, "conf", Config.class );
        inserter.shutdown();
        return config.get( GraphDatabaseSettings.use_memory_mapped_buffers );
    }
}
