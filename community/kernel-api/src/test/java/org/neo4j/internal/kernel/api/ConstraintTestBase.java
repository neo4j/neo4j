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
package org.neo4j.internal.kernel.api;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.values.storable.Values.intValue;

@SuppressWarnings( "Duplicates" )
public abstract class ConstraintTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    protected abstract LabelSchemaDescriptor labelSchemaDescriptor( int labelId, int... propertyIds );

    protected abstract ConstraintDescriptor uniqueConstraintDescriptor( int labelId, int... propertyIds );

    @Before
    public void setup()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            for ( ConstraintDefinition definition : graphDb.schema().getConstraints() )
            {
                definition.drop();
            }
            tx.success();
        }

    }

    @Test
    public void shouldFindConstraintsBySchema() throws Exception
    {
        // GIVEN
        addConstraints( "FOO", "prop" );

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );
            int prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            LabelSchemaDescriptor descriptor = labelSchemaDescriptor( label, prop );

            //WHEN
            List<ConstraintDescriptor> constraints =
                    asList( tx.schemaRead().constraintsGetForSchema( descriptor ) );

            // THEN
            assertThat( constraints, hasSize( 1 ) );
            assertThat( constraints.get( 0 ).schema().getPropertyId(), equalTo( prop ) );
        }
    }

    @Test
    public void shouldFindConstraintsByLabel() throws Exception
    {
        // GIVEN
        addConstraints( "FOO", "prop1", "FOO", "prop2" );

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );

            //WHEN
            List<ConstraintDescriptor> constraints = asList(
                    tx.schemaRead().constraintsGetForLabel( label ) );

            // THEN
            assertThat( constraints, hasSize( 2 ) );
        }
    }

    @Test
    public void shouldBeAbleCheckExistenceOfConstraints() throws Exception
    {
        // GIVEN
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {

            graphDb.schema().constraintFor( label( "FOO" ) ).assertPropertyIsUnique( "prop1" ).create();
            ConstraintDefinition dropped =
                    graphDb.schema().constraintFor( label( "FOO" ) ).assertPropertyIsUnique( "prop2" ).create();
            dropped.drop();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
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
    public void shouldFindAllConstraints() throws Exception
    {
        // GIVEN
        addConstraints( "FOO", "prop1", "BAR", "prop2", "BAZ", "prop3" );

        try ( Transaction tx = session.beginTransaction() )
        {
            //WHEN
            List<ConstraintDescriptor> constraints = asList( tx.schemaRead().constraintsGetAll() );

            // THEN
            assertThat( constraints, hasSize( 3 ) );
        }
    }

    @Test
    public void shouldCheckUniquenessWhenAddingLabel() throws Exception
    {
        // GIVEN
        long nodeConflicting, nodeNotConflicting;
        addConstraints( "FOO", "prop" );
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node conflict = graphDb.createNode();
            conflict.setProperty( "prop", 1337 );
            nodeConflicting = conflict.getId();

            Node ok = graphDb.createNode();
            ok.setProperty( "prop", 42 );
            nodeNotConflicting = ok.getId();

            //Existing node
            Node existing = graphDb.createNode();
            existing.addLabel( Label.label( "FOO" ) );
            existing.setProperty( "prop", 1337 );
            tx.success();
        }

        int label;
        try ( Transaction tx = session.beginTransaction() )
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
            tx.success();
        }

        //Verify
        try ( Transaction tx = session.beginTransaction();
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
    public void shouldCheckUniquenessWhenAddingProperties() throws Exception
    {
        // GIVEN
        long nodeConflicting, nodeNotConflicting;
        addConstraints( "FOO", "prop" );
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node conflict = graphDb.createNode();
            conflict.addLabel( Label.label( "FOO" ) );
            nodeConflicting = conflict.getId();

            Node ok = graphDb.createNode();
            ok.addLabel( Label.label( "BAR" ) );
            nodeNotConflicting = ok.getId();

            //Existing node
            Node existing = graphDb.createNode();
            existing.addLabel( Label.label( "FOO" ) );
            existing.setProperty( "prop", 1337 );
            tx.success();
        }

        int property;
        try ( Transaction tx = session.beginTransaction() )
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
            tx.success();
        }

        //Verify
        try ( Transaction tx = session.beginTransaction();
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
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == key )
            {
                return true;
            }
        }
        return false;
    }

    private void addConstraints( String... labelProps )
    {
        assert labelProps.length % 2 == 0;

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < labelProps.length; i += 2 )
            {
                graphDb.schema().constraintFor( label( labelProps[i] ) ).assertPropertyIsUnique( labelProps[i + 1] )
                        .create();
            }
            tx.success();
        }
    }
}
