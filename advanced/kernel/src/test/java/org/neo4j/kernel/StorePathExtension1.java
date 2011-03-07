/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

public class StorePathExtension1 extends KernelExtension<StorePathState>
{

    static final String EXTENSION_ID = "StorePathExtension";

    public StorePathExtension1()
    {
        super( EXTENSION_ID );
    }

    @Override
    protected StorePathState load( KernelData kernel )
    {
        StorePathState state = new StorePathState();
        state.path = (String) kernel.getParam( Config.STORAGE_DIRECTORY );
        return state;
    }

    @Override
    protected void unload( StorePathState state )
    {
        state.path = null;
    }
}
