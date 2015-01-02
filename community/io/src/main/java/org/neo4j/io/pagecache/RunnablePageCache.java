/**
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
package org.neo4j.io.pagecache;

/**
 * A PageCache that also implements Runnable.
 *
 * The purpose of this is typically for doing page eviction in the background,
 * and the page cache therefore has be started in a dedicated thread.
 *
 * It is implicitly assumed that sending an interrupt to this dedicated thread,
 * will signal it to shut the page cache down.
 */
public interface RunnablePageCache extends PageCache, Runnable
{
}
