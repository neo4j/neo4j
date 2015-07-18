/*
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.MandatoryNodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class MandatoryNodePropertyConstraintCreationIT extends ConstraintCreationIT<MandatoryNodePropertyConstraint>
{
    @Override
    int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException
    {
        return writeOps.labelGetOrCreateForName( name );
    }

    @Override
    MandatoryNodePropertyConstraint createConstraint( SchemaWriteOperations writeOps, int type, int property )
            throws Exception
    {
        return writeOps.mandatoryNodePropertyConstraintCreate( type, property );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        db.schema().constraintFor( label( type ) ).assertPropertyExists( property ).create();
    }

    @Override
    MandatoryNodePropertyConstraint newConstraintObject( int type, int property )
    {
        return new MandatoryNodePropertyConstraint( type, property );
    }

    @Override
    void dropConstraint( SchemaWriteOperations writeOps, MandatoryNodePropertyConstraint constraint ) throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( GraphDatabaseService db )
    {
        db.createNode( label( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label( KEY ) ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }
        }
    }

    @Test
    public void shouldNotDropMandatoryPropertyConstraintThatDoesNotExistWhenThereIsAUniquePropertyConstraint()
            throws Exception
    {
        // given
        UniquenessConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop(
                    new MandatoryNodePropertyConstraint( constraint.label(), constraint.propertyKey() ) );

            fail( "expected exception" );
        }
        // then
        catch ( DropConstraintFailureException e )
        {
            assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
        }
        finally
        {
            rollback();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();

            Iterator<NodePropertyConstraint> constraints = statement
                    .constraintsGetForLabelAndPropertyKey( typeId, propertyKeyId );

            assertEquals( constraint, single( constraints ) );
        }
    }
}
