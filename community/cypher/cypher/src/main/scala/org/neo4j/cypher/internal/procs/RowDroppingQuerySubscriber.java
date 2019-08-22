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
package org.neo4j.cypher.internal.procs;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

public class RowDroppingQuerySubscriber implements QuerySubscriber
{
    private final QuerySubscriber inner;

    public RowDroppingQuerySubscriber( QuerySubscriber inner )
    {
        this.inner = inner;
    }

    @Override
    public void onResult( int numberOfFields ) throws Exception
    {
        inner.onResult( numberOfFields );
    }

    @Override
    public void onRecord() throws Exception
    {
        // drop it
    }

    @Override
    public void onField( AnyValue value ) throws Exception
    {
        // drop it
    }

    @Override
    public void onRecordCompleted() throws Exception
    {
        // drop it
    }

    @Override
    public void onError( Throwable throwable )
    {
        inner.onError( throwable );
    }

    @Override
    public void onResultCompleted( QueryStatistics statistics )
    {
        inner.onResultCompleted( statistics );
    }
}
