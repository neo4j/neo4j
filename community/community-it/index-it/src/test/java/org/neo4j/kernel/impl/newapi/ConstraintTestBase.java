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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.values.storable.Values.intValue;

@SuppressWarnings( "Duplicates" )
public abstract class ConstraintTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    protected abstract LabelSchemaDescriptor labelSchemaDescriptor( int labelId, int... propertyIds );

    protected abstract ConstraintDescriptor uniqueConstraintDescriptor( int labelId, int... propertyIds );

    @BeforeEach
    public void setup()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            for ( ConstraintDefinition definition : tx.schema().getConstraints() )
            {
                definition.drop();
            }
            tx.commit();
        }

    }

    @Test
    void shouldFindConstraintsBySchema() throws Exception
    {
        // GIVEN
        addConstraints( "FOO", "prop" );

        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );
            int prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            LabelSchemaDescriptor descriptor = labelSchemaDescriptor( label, prop );

            //WHEN
            List<ConstraintDescriptor> constraints =
                    asList( tx.schemaRead().constraintsGetForSchema( descriptor ) );

            // THEN
            assertThat( constraints ).hasSize( 1 );
            assertThat( constraints.get( 0 ).schema().getPropertyId() ).isEqualTo( prop );
        }
    }

    @Test
    void shouldFindConstraintsByLabel() throws Exception
    {
        // GIVEN
        addConstraints( "FOO", "prop1", "FOO", "prop2" );

        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );

            //WHEN
            List<ConstraintDescriptor> constraints = asList(
                    tx.schemaRead().constraintsGetForLabel( label ) );

            // THEN
            assertThat( constraints ).hasSize( 2 );
        }
    }

    @Test
    void shouldBeAbleCheckExistenceOfConstraints() throws Exception
    {
        // GIVEN
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {

            tx.schema().constraintFor( label( "FOO" ) ).assertPropertyIsUnique( "prop1" ).create();
            ConstraintDefinition dropped =
                    tx.schema().constraintFor( label( "FOO" ) ).assertPropertyIsUnique( "prop2" ).create();
            dropped.drop();
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );
            int prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
            int prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );

            // THEN
            assertTrue( tx.schemaRead().constraintExists( uniqueConstraintDescriptor( label, prop1 ) ) );
            assertFalse( tx.schemaRead().constraintExists( uniqueConstraintDescriptor( label, prop2 ) ) );
        }
    }

    @Test
    void shouldFindAllConstraints() throws Exception
    {
        // GIVEN
        addConstraints( "FOO", "prop1", "BAR", "prop2", "BAZ", "prop3" );

        try ( KernelTransaction tx = beginTransaction() )
        {
            //WHEN
            List<ConstraintDescriptor> constraints = asList( tx.schemaRead().constraintsGetAll() );

            // THEN
            assertThat( constraints ).hasSize( 3 );
        }
    }

    @Test
    void shouldCheckUniquenessWhenAddingLabel() throws Exception
    {
        // GIVEN
        long nodeConflicting, nodeNotConflicting;
        addConstraints( "FOO", "prop" );
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node conflict = tx.createNode();
            conflict.setProperty( "prop", 1337 );
            nodeConflicting = conflict.getId();

            Node ok = tx.createNode();
            ok.setProperty( "prop", 42 );
            nodeNotConflicting = ok.getId();

            //Existing node
            Node existing = tx.createNode();
            existing.addLabel( Label.label( "FOO" ) );
            existing.setProperty( "prop", 1337 );
            tx.commit();
        }

        int label;
        try ( KernelTransaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );

            //This is ok, since it will satisfy constraint
            assertTrue( tx.dataWrite().nodeAddLabel( nodeNotConflicting, label ) );

            try
            {
                tx.dataWrite().nodeAddLabel( nodeConflicting, label );
                fail();
            }
            catch ( ConstraintValidationException e )
            {
                //ignore
            }
            tx.commit();
        }

        //Verify
        try ( KernelTransaction tx = beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor() )
        {
            //Node without conflict
            tx.dataRead().singleNode( nodeNotConflicting, nodeCursor );
            assertTrue( nodeCursor.next() );
            assertTrue( nodeCursor.labels().contains( label ) );
            //Node with conflict
            tx.dataRead().singleNode( nodeConflicting, nodeCursor );
            assertTrue( nodeCursor.next() );
            assertFalse( nodeCursor.labels().contains( label ) );
        }
    }

    @Test
    void shouldCheckUniquenessWhenAddingProperties() throws Exception
    {
        // GIVEN
        long nodeConflicting, nodeNotConflicting;
        addConstraints( "FOO", "prop" );
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node conflict = tx.createNode();
            conflict.addLabel( Label.label( "FOO" ) );
            nodeConflicting = conflict.getId();

            Node ok = tx.createNode();
            ok.addLabel( Label.label( "BAR" ) );
            nodeNotConflicting = ok.getId();

            //Existing node
            Node existing = tx.createNode();
            existing.addLabel( Label.label( "FOO" ) );
            existing.setProperty( "prop", 1337 );
            tx.commit();
        }

        int property;
        try ( KernelTransaction tx = beginTransaction() )
        {
            property = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );

            //This is ok, since it will satisfy constraint
            tx.dataWrite().nodeSetProperty( nodeNotConflicting, property, intValue( 1337 ) );

            try
            {
                tx.dataWrite().nodeSetProperty( nodeConflicting, property, intValue( 1337 ) );
                fail();
            }
            catch ( ConstraintValidationException e )
            {
                //ignore
            }
            tx.commit();
        }

        //Verify
        try ( KernelTransaction tx = beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor() )
        {
            //Node without conflict
            tx.dataRead().singleNode( nodeNotConflicting, nodeCursor );
            assertTrue( nodeCursor.next() );
            nodeCursor.properties( propertyCursor );
            assertTrue( hasKey( propertyCursor, property ) );
            //Node with conflict
            tx.dataRead().singleNode( nodeConflicting, nodeCursor );
            assertTrue( nodeCursor.next() );
            nodeCursor.properties( propertyCursor );
            assertFalse( hasKey( propertyCursor, property ) );
        }
    }

    private boolean hasKey( PropertyCursor propertyCursor, int key )
    {
       return propertyCursor.seekProperty( key );
    }

    private void addConstraints( String... labelProps )
    {
        assert labelProps.length % 2 == 0;

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < labelProps.length; i += 2 )
            {
                tx.schema().constraintFor( label( labelProps[i] ) ).assertPropertyIsUnique( labelProps[i + 1] )
                        .create();
            }
            tx.commit();
        }
    }
}
