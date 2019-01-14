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

import org.neo4j.storageengine.api.schema.IndexProgressor;

abstract class IndexCursor<T extends IndexProgressor>
{
    private T progressor;

    final void initialize( T progressor )
    {
        if ( this.progressor != null )
        {
            this.progressor.close();
        }
        this.progressor = progressor;
    }

    final boolean innerNext()
    {
        return progressor != null && progressor.next();
    }

    void close()
    {
        if ( progressor != null )
        {
            progressor.close();
        }
        progressor = null;
    }

    boolean isClosed()
    {
        return progressor == null;
    }
}
