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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;

public class NodeKeyConstraintCreationIT extends AbstractConstraintCreationIT<NodeKeyConstraintDescriptor,LabelSchemaDescriptor>
{
    @Override
    int initializeLabelOrRelType( TokenWriteOperations tokenWriteOperations, String name ) throws KernelException
    {
        return tokenWriteOperations.labelGetOrCreateForName( name );
    }

    @Override
    NodeKeyConstraintDescriptor createConstraint( SchemaWriteOperations writeOps, LabelSchemaDescriptor descriptor )
            throws Exception
    {
        return writeOps.nodeKeyConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createNodeKeyConstraint( db, type, property );
    }

    @Override
    NodeKeyConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.nodeKeyForSchema( descriptor );
    }

    @Override
    void dropConstraint( SchemaWriteOperations writeOps, NodeKeyConstraintDescriptor constraint )
            throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( GraphDatabaseService db )
    {
        db.createNode( Label.label( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( Label.label( KEY ) ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }
        }
    }

    @Override
    LabelSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return SchemaDescriptorFactory.forLabel( typeId, propertyKeyId );
    }
}
