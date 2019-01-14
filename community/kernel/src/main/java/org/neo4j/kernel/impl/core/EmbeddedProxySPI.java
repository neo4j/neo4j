/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;

public interface EmbeddedProxySPI
{
    Statement statement();

    KernelTransaction kernelTransaction();

    GraphDatabaseService getGraphDatabase();

    void assertInUnterminatedTransaction();

    void failTransaction();

    RelationshipProxy newRelationshipProxy( long id );

    RelationshipProxy newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId );

    NodeProxy newNodeProxy( long nodeId );

    GraphPropertiesProxy newGraphPropertiesProxy();

    RelationshipType getRelationshipTypeById( int type );

    int getRelationshipTypeIdByName( String typeName );
}
