/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.MandatoryNodePropertyConstraintDefinition;

import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;

public class MandatoryNodePropertyConstraintHaIT extends ConstraintHaIT
{
    @Override
    protected void createConstraint( GraphDatabaseService db, String type, String propertyKey )
    {
        db.schema().constraintFor( label( type ) ).assertPropertyExists( propertyKey ).create();
    }

    @Override
    protected void createEntityInTx( GraphDatabaseService db, String type, String propertyKey, String propertyValue )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( type ) ).setProperty( propertyKey, propertyValue );
            tx.success();
        }
    }

    @Override
    protected void createConstraintViolation( GraphDatabaseService db, String type, String propertyKey,
            String propertyValue )
    {
        db.createNode( label( type ) );
    }

    @Override
    protected void assertConstraintHolds( GraphDatabaseService db, String type, String propertyKey,
            String propertyValue )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> nodes = db.findNodes( label( type ) );
            while ( nodes.hasNext() )
            {
                Node node = nodes.next();
                assertTrue( node.hasProperty( propertyKey ) );
            }
            tx.success();
        }
    }

    @Override
    protected Class<? extends ConstraintDefinition> constraintDefinitionClass()
    {
        return MandatoryNodePropertyConstraintDefinition.class;
    }
}
