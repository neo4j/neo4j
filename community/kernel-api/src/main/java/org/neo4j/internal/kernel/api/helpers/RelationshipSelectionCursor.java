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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.internal.kernel.api.AutoCloseablePlus;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyCursor;

/**
 * Helper cursor for traversing specific types and directions.
 */
public interface RelationshipSelectionCursor extends AutoCloseablePlus
{
    boolean next();

    void setTracer( KernelReadTracer tracer );

    @Override
    void closeInternal();

    long relationshipReference();

    int type();

    long otherNodeReference();

    long sourceNodeReference();

    long targetNodeReference();

    long propertiesReference();

    void properties( PropertyCursor cursor );

    final class Empty extends DefaultCloseListenable implements RelationshipSelectionCursor
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void closeInternal()
        {

        }

        @Override
        public void close()
        {

        }

        @Override
        public long relationshipReference()
        {
            return -1;
        }

        @Override
        public int type()
        {
            return -1;
        }

        @Override
        public long otherNodeReference()
        {
            return -1;
        }

        @Override
        public long sourceNodeReference()
        {
            return -1;
        }

        @Override
        public long targetNodeReference()
        {
            return -1;
        }

        @Override
        public long propertiesReference()
        {
            return -1;
        }

        @Override
        public void properties( PropertyCursor cursor )
        {
            //nothing to do
        }

        @Override
        public boolean isClosed()
        {
            return true;
        }

        @Override
        public void setTracer( KernelReadTracer tracer )
        {
            //do nothing
        }
    }

    RelationshipSelectionCursor EMPTY = new Empty();
}
