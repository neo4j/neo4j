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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;

public class UniquenessConstraintValidationIT extends KernelIntegrationTest
{
    @Test
    public void shouldEnforceUniquenessConstraintOnSetProperty() throws Exception
    {
        // given
        newTransaction();
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.labelGetForName( getState(), "Foo" );
        long name = statement.propertyKeyGetForName( getState(), "name" );
        commit();
        newTransaction();
        statement.uniquenessConstraintCreate( getState(), foo, name );
        commit();

        newTransaction();

        // when
        Node node = db.createNode( label( "Foo" ) );
        try
        {
            node.setProperty( "name", "foo" );

            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationException ex )
        {
            assertThat( ex.getMessage(), containsString( "\"name\"=[foo]" ) );
        }
    }

    @Test
    public void shouldEnforceUniquenessConstraintOnAddLabel() throws Exception
    {
        // given
        newTransaction();
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.labelGetForName( getState(), "Foo" );
        long name = statement.propertyKeyGetForName( getState(), "name" );
        commit();
        newTransaction();
        statement.uniquenessConstraintCreate( getState(), foo, name );
        commit();

        newTransaction();

        // when
        Node node = db.createNode();
        node.setProperty( "name", "foo" );
        try
        {
            node.addLabel( label( "Foo" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationException ex )
        {
            assertThat( ex.getMessage(), containsString( "\"name\"=[foo]" ) );
        }
    }
}
