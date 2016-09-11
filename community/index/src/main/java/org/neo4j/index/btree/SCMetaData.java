/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.btree;

import org.neo4j.index.SCIndexDescription;

/**
 * META DATA FORMAT
 *
 * Firstlabel
 * REALTIONSHIPTYPE DIRECTION [propertyKey]
 * Secondlabel [propertyKey]
 * pagesize
 * rootid
 *
 * example
 * Person
 * CREATED OUTGOING
 * Comment date
 * 8192
 * 0
 */
public class SCMetaData
{
    public final SCIndexDescription description;
    public final int pageSize;
    public final long rootId;
    public final long lastId;

    public SCMetaData( SCIndexDescription description, int pageSize, long rootId, long lastId )
    {
        this.description = description;
        this.pageSize = pageSize;
        this.rootId = rootId;
        this.lastId = lastId;
    }
}
