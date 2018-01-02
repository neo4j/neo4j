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
package org.neo4j.legacy.consistency.store.paging;

import org.neo4j.legacy.consistency.store.paging.Page;
import org.neo4j.legacy.consistency.store.paging.PageLoadFailureException;
import org.neo4j.legacy.consistency.store.paging.PageReplacementStrategy;

public class StubPageReplacementStrategy implements PageReplacementStrategy
{
    @Override
    public <PAYLOAD, PAGE extends Page<PAYLOAD>> PAYLOAD acquire( PAGE page, Storage<PAYLOAD, PAGE> storage )
            throws PageLoadFailureException
    {
        return page.payload = storage.load( page );
    }

    @Override
    public <PAYLOAD> void forceEvict( Page<PAYLOAD> page )
    {
        page.evict();
    }
}
