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
package org.neo4j.server.rest.web;

public interface Surface
{
    String PATH_NODES = "node";
    String PATH_NODE_INDEX = "index/node";
    String PATH_RELATIONSHIP_INDEX = "index/relationship";
    String PATH_EXTENSIONS = "ext";
    String PATH_RELATIONSHIP_TYPES = "relationship/types";
    String PATH_SCHEMA_INDEX = "schema/index";
    String PATH_SCHEMA_CONSTRAINT = "schema/constraint";
    String PATH_SCHEMA_RELATIONSHIP_CONSTRAINT = "schema/relationship/constraint";
    String PATH_BATCH = "batch";
    String PATH_CYPHER = "cypher";
    String PATH_TRANSACTION = "transaction";
    String PATH_RELATIONSHIPS = "relationship";
    String PATH_LABELS = "labels";
}
