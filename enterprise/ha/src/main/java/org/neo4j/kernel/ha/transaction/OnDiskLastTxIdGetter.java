/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.io.File;

import org.neo4j.kernel.ha.NeoStoreUtil;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class OnDiskLastTxIdGetter implements LastTxIdGetter
{
    private final File storeDirectory;

    public OnDiskLastTxIdGetter( File storeDirectory )
    {
        this.storeDirectory = storeDirectory;
    }

    @Override
    public long getLastTxId()
    {
        if ( new File(storeDirectory, NeoStore.DEFAULT_NAME).exists() )
        {
            return new NeoStoreUtil(storeDirectory).getLastCommittedTx();
        }
        else
        {
            return -1;
        }
    }
}
