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

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProgressor;

/**
 * Similar to an {@link IndexProgressor.EntityValueClient}, but will be given a {@link Read} instance, and possibly a {@link Resource}, prior to its
 * {@link #initialize(IndexDescriptor, IndexProgressor, IndexQuery[], IndexOrder, boolean, boolean)}  initialization}.
 * <p>
 * This is useful if the entity references needs to be processed further.
 */
public interface EntityIndexSeekClient extends IndexProgressor.EntityValueClient
{
    void setRead( Read read );
}
