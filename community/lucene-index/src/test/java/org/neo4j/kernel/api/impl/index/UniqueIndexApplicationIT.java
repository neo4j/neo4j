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
package org.neo4j.kernel.api.impl.index;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.Function;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.test.DatabaseFunctions.addLabel;
import static org.neo4j.test.DatabaseFunctions.createNode;
import static org.neo4j.test.DatabaseFunctions.index;
import static org.neo4j.test.DatabaseFunctions.setProperty;
import static org.neo4j.test.DatabaseFunctions.uniquenessConstraint;

@RunWith(Parameterized.class)
public class UniqueIndexApplicationIT
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> indexTypes()
    {
        return asList( createIndex( index( label( "Label1" ), "key1" ) ),
                       createIndex( uniquenessConstraint( label( "Label1" ), "key1" ) ) );
    }

    private final Function<GraphDatabaseService, ?> createIndex;

    @Before
    public void given() throws Exception
    {
        db.executeAndCommit( createIndex );
    }

    @Test
    public void tx_createNode_addLabel_setProperty() throws Exception
    {
        db.when( db.tx(
                createNode().then( addLabel( label( "Label1" ) ).then( setProperty( "key1", "value1" ) ) )
        ) );
    }

    @Test
    public void tx_createNode_tx_addLabel_setProperty() throws Exception
    {
        db.when( db.tx(
                createNode()
        ).then( db.tx(
                addLabel( label( "Label1" ) ).then( setProperty( "key1", "value1" ) )
        ) ) );
    }

    @Test
    public void tx_createNode_addLabel_tx_setProperty() throws Exception
    {
        db.when( db.tx(
                createNode().then( addLabel( label( "Label1" ) ) )
        ).then( db.tx(
                setProperty( "key1", "value1" )
        ) ) );
    }

    @Test
    public void tx_createNode_setProperty_tx_addLabel() throws Exception
    {
        db.when( db.tx(
                createNode().then( setProperty( "key1", "value1" ) )
        ).then( db.tx(
                addLabel( label( "Label1" ) )
        ) ) );
    }

    @Test
    public void tx_createNode_tx_addLabel_tx_setProperty() throws Exception
    {
        db.when( db.tx(
                createNode()
        ).then( db.tx(
                addLabel( label( "Label1" ) )
        ).then( db.tx(
                setProperty( "key1", "value1" ) )
        ) ) );
    }

    @Test
    public void tx_createNode_tx_setProperty_tx_addLabel() throws Exception
    {
        db.when( db.tx(
                createNode()
        ).then( db.tx(
                setProperty( "key1", "value1" )
        ).then( db.tx(
                addLabel( label( "Label1" ) )
        ) ) ) );
    }

    @After
    public void then() throws Exception
    {
        assertEquals( "number of matching nodes in the index",
                      1, (int) db.when( db.tx( countNodesFromIndexLookup( label( "Label1" ), "key1", "value1" ) ) ) );
    }

    private static Function<GraphDatabaseService, Integer> countNodesFromIndexLookup(
            final Label label, final String propertyKey, final Object value )
    {
        return new Function<GraphDatabaseService, Integer>()
        {
            @Override
            public Integer apply( GraphDatabaseService graphDb )
            {
                return count( graphDb.findNodesByLabelAndProperty( label, propertyKey, value ) );
            }
        };
    }

    public UniqueIndexApplicationIT( Function<GraphDatabaseService, ?> createIndex )
    {
        this.createIndex = createIndex;
    }

    private static Object[] createIndex( Function<GraphDatabaseService, Void> createIndex )
    {
        return new Object[]{createIndex};
    }
}