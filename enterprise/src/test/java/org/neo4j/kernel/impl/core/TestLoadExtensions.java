/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.management.Primitives;

public class TestLoadExtensions
{
    private EmbeddedGraphDatabase db;

    @Before
    public void startDb()
    {
        db = new EmbeddedGraphDatabase( "target/var/ext" );
    }
    
    @After
    public void shutdownDb()
    {
        db.shutdown();
    }
    
    @Test
    public void makeSureJmxExtensionCanBeLoadedAndQueried()
    {
        Primitives primitivesBean = ((AbstractGraphDatabase) db).getManagementBean( Primitives.class );
        
        // Just assert so that no exception is thrown.
        primitivesBean.getNumberOfNodeIdsInUse();
    }
}
