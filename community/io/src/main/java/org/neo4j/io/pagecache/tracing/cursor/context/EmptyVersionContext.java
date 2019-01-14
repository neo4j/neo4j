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
package org.neo4j.io.pagecache.tracing.cursor.context;

/**
 * {@link VersionContext} that does not perform any kind of version tracking for cases when its not required.
 * @see VersionContext
 */
public class EmptyVersionContext implements VersionContext
{
    public static final VersionContext EMPTY = new EmptyVersionContext();

    private EmptyVersionContext()
    {
    }

    @Override
    public void initRead()
    {
    }

    @Override
    public void initWrite( long committingTransactionId )
    {
    }

    @Override
    public long committingTransactionId()
    {
        return 0;
    }

    @Override
    public long lastClosedTransactionId()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void markAsDirty()
    {
    }

    @Override
    public boolean isDirty()
    {
        return false;
    }
}
