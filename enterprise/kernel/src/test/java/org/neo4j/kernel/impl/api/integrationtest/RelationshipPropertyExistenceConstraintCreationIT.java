/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class RelationshipPropertyExistenceConstraintCreationIT
        extends AbstractConstraintCreationIT<RelationshipPropertyExistenceConstraint>
{
    @Override
    int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException
    {
        return writeOps.relationshipTypeGetOrCreateForName( name );
    }

    @Override
    RelationshipPropertyExistenceConstraint createConstraint( SchemaWriteOperations writeOps, int type,
            int property ) throws Exception
    {
        return writeOps.relationshipPropertyExistenceConstraintCreate( type, property );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createRelPropertyExistenceConstraint( db, type, property );
    }

    @Override
    RelationshipPropertyExistenceConstraint newConstraintObject( int type, int property )
    {
        return new RelationshipPropertyExistenceConstraint( type, property );
    }

    @Override
    void dropConstraint( SchemaWriteOperations writeOps, RelationshipPropertyExistenceConstraint constraint )
            throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( GraphDatabaseService db )
    {
        Node start = db.createNode();
        Node end = db.createNode();
        start.createRelationshipTo( end, withName( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        Iterable<Relationship> relationships = GlobalGraphOperations.at( db ).getAllRelationships();
        for ( Relationship relationship : relationships )
        {
            relationship.delete();
        }
    }
}
