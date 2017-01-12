/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
/**
 * B+tree implementation with arbitrary key/value. Index implementation is
 * {@link org.neo4j.index.internal.gbptree.GBPTree}, which works on a {@link org.neo4j.io.pagecache.PageCache}.
 * Implementation supports single writer together with concurrent lock-free and garbage-free readers.
 * <p>
 * To create an index with a custom layout (type of key/value), implement a custom
 * {@link org.neo4j.index.internal.gbptree.Layout}.
 * <p>
 * See https://en.wikipedia.org/wiki/B%2B_tree
 */
package org.neo4j.index.internal.gbptree;
