/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;

import static org.neo4j.graphdb.RelationshipType.withName;

public class RelationshipPropertyExistenceConstraintCreationIT
        extends AbstractConstraintCreationIT<RelationshipPropertyExistenceConstraint,RelationshipPropertyDescriptor>
{
    @Override
    int initializeLabelOrRelType( TokenWriteOperations tokenWriteOperations, String name ) throws KernelException
    {
        return tokenWriteOperations.relationshipTypeGetOrCreateForName( name );
    }

    @Override
    RelationshipPropertyExistenceConstraint createConstraint( SchemaWriteOperations writeOps,
            RelationshipPropertyDescriptor descriptor ) throws Exception
    {
        return writeOps.relationshipPropertyExistenceConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createRelPropertyExistenceConstraint( db, type, property );
    }

    @Override
    RelationshipPropertyExistenceConstraint newConstraintObject( RelationshipPropertyDescriptor descriptor )
    {
        return new RelationshipPropertyExistenceConstraint( descriptor );
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
        Iterable<Relationship> relationships = db.getAllRelationships();
        for ( Relationship relationship : relationships )
        {
            relationship.delete();
        }
    }

    @Override
    RelationshipPropertyDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return new RelationshipPropertyDescriptor( typeId, propertyKeyId );
    }
}
