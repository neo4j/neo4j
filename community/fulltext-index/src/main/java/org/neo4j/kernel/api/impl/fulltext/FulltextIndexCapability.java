/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.internal.schema.IndexBehaviour;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexOrderCapability;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.values.storable.ValueCategory;

class FulltextIndexCapability implements IndexCapability
{
    private static final IndexBehaviour[] EVENTUALLY_CONSISTENT_BEHAVIOUR = {IndexBehaviour.EVENTUALLY_CONSISTENT, IndexBehaviour.SKIP_AND_LIMIT};
    private static final IndexBehaviour[] NORMAL_BEHAVIOUR = {IndexBehaviour.SKIP_AND_LIMIT};

    private final boolean isEventuallyConsistent;

    FulltextIndexCapability( boolean isEventuallyConsistent )
    {
        this.isEventuallyConsistent = isEventuallyConsistent;
    }

    @Override
    public IndexOrderCapability orderCapability( ValueCategory... valueCategories )
    {
        return IndexOrderCapability.NONE;
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        return IndexValueCapability.NO;
    }

    @Override
    public IndexBehaviour[] behaviours()
    {
        return isEventuallyConsistent ? EVENTUALLY_CONSISTENT_BEHAVIOUR : NORMAL_BEHAVIOUR;
    }
}
