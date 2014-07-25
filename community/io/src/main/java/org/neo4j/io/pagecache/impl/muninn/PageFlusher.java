/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIntVisitor;
import org.neo4j.io.pagecache.PageSwapper;

public class PageFlusher implements PrimitiveLongIntVisitor
{
    private final MuninnPage[] cachePages;
    private final PageSwapper swapper;

    public PageFlusher( MuninnPage[] cachePages, PageSwapper swapper )
    {
        this.cachePages = cachePages;
        this.swapper = swapper;
    }

    @Override
    public void visited( long filePageId, int cachePageId )
    {
        MuninnPage page = cachePages[cachePageId];
        long stamp = page.writeLock();
        try
        {
            page.flush( swapper, filePageId );
        }
        catch ( IOException e )
        {
            // TODO throw something better, here...
            throw new RuntimeException( e );
        }
        finally
        {
            page.unlockWrite( stamp );
        }
    }
}
