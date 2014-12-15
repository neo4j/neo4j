/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.NodeManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BridgingCacheAccessTest
{
    private NodeManager nodeManager;
    private SchemaCache schemaCache;
    private SchemaState schemaState;
    private PersistenceCache persistenceCache;
    private CacheAccessBackDoor access;

    @Before
    public void setUp()
    {
        nodeManager = mock( NodeManager.class );
        schemaCache = mock( SchemaCache.class );
        schemaState = mock( SchemaState.class );
        persistenceCache = mock( PersistenceCache.class );
        access = new BridgingCacheAccess( nodeManager, schemaCache, schemaState, persistenceCache );
    }

    @Test
    public void removingSchemaRuleMustClearSchemaState()
    {
        // when
        access.removeSchemaRuleFromCache( 1 );

        // then
        verify( schemaState ).clear();
    }
}
