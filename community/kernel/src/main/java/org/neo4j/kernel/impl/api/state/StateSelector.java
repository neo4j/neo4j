/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongObjectMap;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.VersionedPrimitiveLongDiffSets;
import org.neo4j.values.storable.Value;

public interface StateSelector
{

    StateSelector CURRENT_STATE = new StateSelector()
    {
        public PrimitiveLongDiffSets getView( VersionedPrimitiveLongDiffSets diffSets )
        {
            return diffSets.currentView();
        }

        public PrimitiveLongObjectMap<Value> getView( VersionedPrimitiveLongObjectMap<Value> map )
        {
            return map.currentView();
        }
    };

    StateSelector STABLE_STATE = new StateSelector()
    {
        public PrimitiveLongDiffSets getView( VersionedPrimitiveLongDiffSets diffSets )
        {
            return diffSets.stableView();
        }

        public PrimitiveLongObjectMap<Value> getView( VersionedPrimitiveLongObjectMap<Value> map )
        {
            return map.stableView();
        }
    };

    PrimitiveLongDiffSets getView( VersionedPrimitiveLongDiffSets diffSets );

    PrimitiveLongObjectMap<Value> getView( VersionedPrimitiveLongObjectMap<Value> map );

}
