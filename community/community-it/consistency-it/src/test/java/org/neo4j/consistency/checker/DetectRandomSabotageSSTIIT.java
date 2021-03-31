/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checker;

import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;

public class DetectRandomSabotageSSTIIT extends DetectRandomSabotageIT
{
    @Override
    protected <T> T addConfig( T t, SetConfigAction<T> action )
    {
        action.setConfig( t, RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
        return t;
    }

    @Override
    DetectRandomSabotageIT.SabotageType[] getValidSabotageTypes()
    {
        // Sabotage only the two token indexes
        return new SabotageType[]{
                SabotageType.NODE_LABEL_INDEX_ENTRY,
                SabotageType.RELATIONSHIP_TYPE_INDEX_ENTRY
        };
    }
}
