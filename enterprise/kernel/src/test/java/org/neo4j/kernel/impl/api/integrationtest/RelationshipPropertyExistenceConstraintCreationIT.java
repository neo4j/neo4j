/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.api.integrationtest;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.RelExistenceConstraintDescriptor;

import static org.neo4j.graphdb.RelationshipType.withName;

public class RelationshipPropertyExistenceConstraintCreationIT
        extends AbstractConstraintCreationIT<ConstraintDescriptor,RelationTypeSchemaDescriptor>
{
    @Override
    int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException
    {
        return tokenWrite.relationshipTypeGetOrCreateForName( name );
    }

    @Override
    ConstraintDescriptor createConstraint( SchemaWrite writeOps,
            RelationTypeSchemaDescriptor descriptor ) throws Exception
    {
        return writeOps.relationshipPropertyExistenceConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createRelPropertyExistenceConstraint( db, type, property );
    }

    @Override
    RelExistenceConstraintDescriptor newConstraintObject( RelationTypeSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.existsForSchema( descriptor );
    }

    @Override
    void dropConstraint( SchemaWrite writeOps, ConstraintDescriptor constraint )
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
    RelationTypeSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return SchemaDescriptorFactory.forRelType( typeId, propertyKeyId );
    }
}
