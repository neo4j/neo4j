/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.impl.EphemeralIdGenerator;

public class LabelsAcceptanceTest
{
    public @Rule ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL;
    }

    @Test
    public void addingALabelUsingAValidIdentifierShouldSucceed() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = null;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel(Labels.MY_LABEL);

            tx.success();
        } finally
        {
            tx.finish();
        }

        // Then
        assertTrue( "Label should have been added to node", myNode.hasLabel( Labels.MY_LABEL ) );
    }

    @Test
    public void addingALabelUsingAnInvalidIdentifierShouldFail() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Exception caught = null;

        // When I set an empty label
        Transaction tx = beansAPI.beginTx();
        try
        {
            beansAPI.createNode().addLabel( label( "" ));
        } catch(ConstraintViolationException ex)
        {
            caught = ex;
        } finally
        {
            tx.finish();
        }

        // Then
        assertThat( "Correct exception should have been thrown.", caught, is(ConstraintViolationException.class));

        // And When I set a null label
        Transaction tx2 = beansAPI.beginTx();
        try
        {
            beansAPI.createNode().addLabel( label( null ));
        } catch(ConstraintViolationException ex)
        {
            caught = ex;
        } finally
        {
            tx2.finish();
        }

        // Then
        assertThat( "Correct exception should have been thrown.", caught, is(ConstraintViolationException.class));
    }

    @Test
    public void addingALabelThatAlreadyExistsBehavesAsNoOp() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = null;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel(Labels.MY_LABEL);
            myNode.addLabel(Labels.MY_LABEL);

            tx.success();
        } finally
        {
            tx.finish();
        }

        // Then
        assertTrue( "Label should have been added to node", myNode.hasLabel( Labels.MY_LABEL ) );
        // TODO: When support for reading labels has been introduced, assert that MY_LABEL occurs only once
    }

    @Test
    public void oversteppingMaxNumberOfLabelsShouldFailGracefully() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = beansAPIWithNoMoreLabelIds();
        Exception caught = null;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            beansAPI.createNode().addLabel(Labels.MY_LABEL);
        } catch (ConstraintViolationException ex)
        {
            caught = ex;
        } finally
        {
            tx.finish();
        }

        // Then
        assertThat( "IllegalLabelException should have been thrown.", caught,is( ConstraintViolationException.class));
    }

    private GraphDatabaseService beansAPIWithNoMoreLabelIds()
    {
        return new ImpermanentGraphDatabase(  ) {
            @Override
            protected IdGeneratorFactory createIdGeneratorFactory()
            {
                return new EphemeralIdGenerator.Factory()
                {
                    @Override
                    public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType,
                                             long highId )
                    {
                        switch(idType)
                        {
                            case PROPERTY_INDEX:
                                return new EphemeralIdGenerator( idType )
                                {
                                    @Override public long nextId()
                                    {
                                        // Same exception as the one thrown by IdGeneratorImpl
                                        throw new UnderlyingStorageException( "Id capacity exceeded" );
                                    }
                                };
                            default:
                                return super.open( fs, fileName, grabSize, idType, Long.MAX_VALUE );
                        }
                    }
                };
            }
        };
    }
}
