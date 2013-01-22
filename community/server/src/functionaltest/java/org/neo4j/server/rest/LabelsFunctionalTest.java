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
package org.neo4j.server.rest;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.LABEL;
import org.neo4j.test.GraphDescription.NODE;

public class LabelsFunctionalTest  extends AbstractRestFunctionalTestBase
{
    /**
     * Adding a label to a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", setNameProperty = true ) } )
    public void adding_a_label_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "I" ) );

        gen.get()
            .expectedStatus( 204 )
            .payload( createJsonFrom( "MyLabel" ) )
            .post( nodeUri + "/labels"  );
    }

    /**
     * Adding a label with an invalid name leads to a 400 response. Labels with empty names are not allowed,
     * however, all other valid strings are accepted as label names.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", setNameProperty = true ) } )
    public void adding_an_invalid_label_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "I" ) );

        gen.get()
            .expectedStatus( 400 )
            .payload( createJsonFrom( "" ) )
            .post( nodeUri + "/labels"  );
    }
    
    /**
     * Listing a Node's labels.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", labels = { @LABEL( "Me" ), @LABEL( "You" ) }, setNameProperty = true ) } )
    public void listing_node_labels() throws PropertyValueException
    {
        Map<String, Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "I" ) );

        String body = gen.get()
            .expectedStatus( 200 )
            .get( nodeUri + "/labels"  )
            .entity();
        List<String> labels = (List<String>) JsonHelper.readJson( body );
        assertEquals( asSet( "Me", "You" ), asSet( labels ) );
    }
}
