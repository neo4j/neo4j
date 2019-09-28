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
package org.neo4j.kernel.api.impl.index;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ImpermanentDbmsExtension
class AccidentalUniquenessConstraintViolationIT
{
    private static final Label Foo = Label.label( "Foo" );
    private static final String BAR = "bar";

    private static Stream<Arguments> parameters()
    {
        return Stream.of( Arguments.of( 42, 41 ), Arguments.of( "a", "b" ) );
    }

    @Inject
    private GraphDatabaseService db;

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldApplyChangesWithIntermediateConstraintViolations( Object value1, Object value2 )
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( Foo ).assertPropertyIsUnique( BAR ).create();
            tx.commit();
        }
        Node fourtyTwo;
        Node fourtyOne;
        try ( Transaction tx = db.beginTx() )
        {
            fourtyTwo = tx.createNode( Foo );
            fourtyTwo.setProperty( BAR, value1 );
            fourtyOne = tx.createNode( Foo );
            fourtyOne.setProperty( BAR, value2 );
            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( fourtyOne.getId() ).delete();
            tx.getNodeById( fourtyTwo.getId() ).setProperty( BAR, value2 );
            tx.commit();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            fourtyTwo = tx.getNodeById( fourtyTwo.getId() );
            assertEquals( value2, fourtyTwo.getProperty( BAR ) );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( fourtyOne.getId() ).getProperty( BAR ) );

            assertEquals( fourtyTwo, tx.findNode( Foo, BAR, value2 ) );
            assertNull( tx.findNode( Foo, BAR, value1 ) );
            tx.commit();
        }
    }
}
