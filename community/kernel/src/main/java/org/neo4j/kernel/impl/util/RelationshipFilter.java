/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

/**
 * Able to filter relationship additions based on type, direction and which id is the first id of already
 * existing ids.
 */
public interface RelationshipFilter
{
    /**
     * @param type relationship type token id.
     * @param direction direction of the relationship.
     * @param firstCachedId first existing cached id, for comparison.
     * @return whether or not adding relationships, given the information above, is accepted.
     */
    boolean accept( int type, DirectionWrapper direction, long firstCachedId );

    public static final RelationshipFilter ACCEPT_ALL = new RelationshipFilter()
    {
        @Override
        public boolean accept( int type, DirectionWrapper direction, long firstCachedId )
        {
            return true;
        }
    };
}
