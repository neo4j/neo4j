/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.consistency.repair;

import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.SOURCE_NODE_INVALID;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.SOURCE_NODE_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.TARGET_NODE_INVALID;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.TARGET_NODE_NOT_IN_USE;

import org.neo4j.consistency.checking.old.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public enum RelationshipNodeField
{
    FIRST( SOURCE_NODE_INVALID, SOURCE_NODE_NOT_IN_USE )
    {
        @Override
        public long get( RelationshipRecord rel )
        {
            return rel.getFirstNode();
        }
    },
    SECOND( TARGET_NODE_INVALID, TARGET_NODE_NOT_IN_USE )
    {
        @Override
        public long get( RelationshipRecord rel )
        {
            return rel.getSecondNode();
        }
    };
    public final InconsistencyType.ReferenceInconsistency invalidReference, notInUse;

    public abstract long get( RelationshipRecord rel );

    RelationshipNodeField( InconsistencyType.ReferenceInconsistency invalidReference,
                           InconsistencyType.ReferenceInconsistency notInUse )
    {
        this.invalidReference = invalidReference;
        this.notInUse = notInUse;
    }
}
