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
/**
 * Contains {@link org.neo4j.index.lucene.QueryContext} and
 * {@link org.neo4j.index.lucene.ValueContext} which is part of the
 * public API for an {@link org.neo4j.graphdb.index.Index} backed by Lucene.
 * 
 * Also contain a timeline implementation on top of an
 * {@link org.neo4j.graphdb.index.Index} based on Lucene.
 */
package org.neo4j.index.lucene;
