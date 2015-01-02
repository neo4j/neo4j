/**
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
package org.neo4j.server.rest;

import java.util.List;

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.RestfulGraphDatabase;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutoIndexDocIT extends AbstractRestFunctionalTestBase
{
    /**
     * Find node by query from an automatic index.
     * 
     * See Find node by query for the actual query syntax.
     */
    @Documented
    @Test
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true ) }, autoIndexNodes = true )
    public void shouldRetrieveFromAutoIndexByQuery()
    {
        data.get();
        assertSize( 1, gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get( nodeAutoIndexUri() + "?query=name:I" )
                .entity() );
    }

    private String nodeAutoIndexUri()
    {
        return getDataUri() + "index/auto/node/";
    }

    /**
     * Automatic index nodes can be found via exact lookups with normal Index
     * REST syntax.
     */
    @Documented
    @Test
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true ) }, autoIndexNodes = true )
    public void find_node_by_exact_match_from_an_automatic_index()
    {
        data.get();
        assertSize( 1, gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get( nodeAutoIndexUri() + "name/I" )
                .entity() );
    }

    /**
     * The automatic relationship index can not be removed.
     */
    @Test
    @Documented
    @Title( "Relationship AutoIndex is not removable" )
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true ) }, autoIndexNodes = true )
    public void Relationship_AutoIndex_is_not_removable()
    {
        data.get();
        gen.get()
                .noGraph()
                .expectedStatus( 405 )
                .delete( relationshipAutoIndexUri() )
                .entity();
    }

    /**
     * The automatic node index can not be removed.
     */
    @Test
    @Documented
    @Title( "Node AutoIndex is not removable" )
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true ) }, autoIndexNodes = true )
    public void AutoIndex_is_not_removable()
    {
        gen.get()
                .noGraph()
                .expectedStatus( 405 )
                .delete( nodeAutoIndexUri() )
                .entity();
    }

    /**
     * It is not allowed to add items manually to automatic indexes.
     */
    @Test
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true ) }, autoIndexNodes = true )
    @Documented
    @Title( "Items can not be added manually to an node AutoIndex" )
    public void items_can_not_be_added_manually_to_an_AutoIndex() throws Exception
    {
        data.get();
        String indexName = graphdb().index()
                .getNodeAutoIndexer()
                .getAutoIndex()
                .getName();

        gen.get()
                .noGraph()
                .expectedStatus( 405 )
                .payload( createJsonStringFor( getNodeUri( data.get()
                        .get( "I" ) ), "name", "I" ) )
                .post( postNodeIndexUri( indexName ) )
                .entity();

    }

    private String createJsonStringFor( final String targetUri, final String key, final String value )
    {
        return "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"uri\": \"" + targetUri + "\"}";
    }

    /**
     * It is not allowed to add items manually to automatic indexes.
     */
    @Test
    @Graph( nodes = { @NODE( name = "I" ), @NODE( name = "you" ) }, relationships = { @REL( start = "I", end = "you", type = "know", properties = { @PROP( key = "since", value = "today" ) } ) }, autoIndexRelationships = true )
    @Documented
    @Title( "Items can not be added manually to a relationship AutoIndex" )
    public void items_can_not_be_added_manually_to_a_Relationship_AutoIndex() throws Exception
    {
        data.get();
        String indexName = graphdb().index()
                .getRelationshipAutoIndexer()
                .getAutoIndex()
                .getName();
        try ( Transaction tx = graphdb().beginTx() )
        {
            gen.get()
                    .noGraph()
                    .expectedStatus( 405 )
                    .payload( createJsonStringFor( getRelationshipUri( data.get()
                            .get( "I" )
                            .getRelationships()
                            .iterator()
                            .next() ), "name", "I" ) )
                    .post( postRelationshipIndexUri( indexName ) )
                    .entity();
        }
    }

    /**
     * It is not allowed to remove entries manually from automatic indexes.
     */
    @Test
    @Documented
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true ) }, autoIndexNodes = true )
    @Title( "Automatically indexed nodes cannot be removed from the index manually" )
    public void autoindexed_items_cannot_be_removed_manually()
    {
        long id = data.get()
                .get( "I" )
                .getId();
        String indexName = graphdb().index()
                .getNodeAutoIndexer()
                .getAutoIndex()
                .getName();
        gen.get()
                .noGraph()
                .expectedStatus( 405 )
                .delete( getDataUri() + "index/node/" + indexName + "/name/I/" + id )
                .entity();
        gen.get()
                .noGraph()
                .expectedStatus( 405 )
                .delete( getDataUri() + "index/node/" + indexName + "/name/" + id )
                .entity();
        gen.get()
                .noGraph()
                .expectedStatus( 405 )
                .delete( getDataUri() + "index/node/" + indexName + "/" + id )
                .entity();
    }

    /**
     * It is not allowed to remove entries manually from automatic indexes.
     */
    @Test
    @Documented
    @Graph( nodes = { @NODE( name = "I" ), @NODE( name = "you" ) }, relationships = { @REL( start = "I", end = "you", type = "know", properties = { @PROP( key = "since", value = "today" ) } ) }, autoIndexRelationships = true )
    @Title( "Automatically indexed relationships cannot be removed from the index manually" )
    public void autoindexed_relationships_cannot_be_removed_manually()
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            long id = data.get()
                    .get( "I" )
                    .getRelationships()
                    .iterator()
                    .next()
                    .getId();
            String indexName = graphdb().index()
                    .getRelationshipAutoIndexer()
                    .getAutoIndex()
                    .getName();
            gen.get()
                    .noGraph()
                    .expectedStatus( 405 )
                    .delete( getDataUri() + "index/relationship/" + indexName + "/since/today/" + id )
                    .entity();
            gen.get()
                    .noGraph()
                    .expectedStatus( 405 )
                    .delete( getDataUri() + "index/relationship/" + indexName + "/since/" + id )
                    .entity();
            gen.get()
                    .noGraph()
                    .expectedStatus( 405 )
                    .delete( getDataUri() + "index/relationship/" + indexName + "/" + id )
                    .entity();
        }
    }

    /**
     * See the example request.
     */
    @Documented
    @Title( "Find relationship by query from an automatic index" )
    @Test
    @Graph( nodes = { @NODE( name = "I" ), @NODE( name = "you" ) }, relationships = { @REL( start = "I", end = "you", type = "know", properties = { @PROP( key = "since", value = "today" ) } ) }, autoIndexRelationships = true )
    public void Find_relationship_by_query_from_an_automatic_index()
    {
        data.get();
        assertSize( 1, gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get( relationshipAutoIndexUri() + "?query=since:today" )
                .entity() );
    }

    /**
     * See the example request.
     */
    @Documented
    @Title( "Find relationship by exact match from an automatic index" )
    @Test
    @Graph( nodes = { @NODE( name = "I" ), @NODE( name = "you" ) }, relationships = { @REL( start = "I", end = "you", type = "know", properties = { @PROP( key = "since", value = "today" ) } ) }, autoIndexRelationships = true )
    public void Find_relationship_by_exact_match_from_an_automatic_index()
    {
        data.get();
        assertSize( 1, gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get( relationshipAutoIndexUri() + "since/today/" )
                .entity() );
    }
    
    /**
     * Get current status for autoindexing on nodes.
     */
    @Test
    @Documented
    public void getCurrentStatusForNodes() {
        checkAndAssertAutoIndexerIsEnabled("node", false);
    }

    /**
     * Enable node autoindexing.
     */
    @Test
    @Documented
    public void enableNodeAutoIndexing() {
        setEnabledAutoIndexingForType("node", true);
    }

    /**
     * Add a property for autoindexing on nodes.
     */
    @Test
    @Documented
    public void addAutoIndexingPropertyForNodes() {
        gen.get()
                .noGraph()
                .expectedStatus( 204 )
                .payload( "myProperty1" )
                .post( autoIndexURI( "node" ) + "/properties" );
    }

    /**
     * Lookup list of properties being autoindexed.
     */
    @Test
    @Documented
    public void listAutoIndexingPropertiesForNodes() throws JsonParseException {
        String propName = "some-property";
        server().getDatabase().getGraph().index().getNodeAutoIndexer().startAutoIndexingProperty(propName);
        
        List<String> properties = getAutoIndexedPropertiesForType("node");
        
        assertEquals(1, properties.size());
        assertEquals(propName, properties.get(0));
    }

    /**
     * Remove a property for autoindexing on nodes.
     */
    @Test
    @Documented
    public void removeAutoIndexingPropertyForNodes() {
        gen.get()
                .noGraph()
                .expectedStatus( 204 )
                .delete( autoIndexURI( "node" ) + "/properties/myProperty1" );
    }

    @Test
    public void switchOnOffAutoIndexingForNodes() {
        switchOnOffAutoIndexingForType("node");
    }

    @Test
    public void switchOnOffAutoIndexingForRelationships() {
        switchOnOffAutoIndexingForType("relationship");
    }

    @Test
    public void addRemoveAutoIndexedPropertyForNodes() throws JsonParseException {
        addRemoveAutoIndexedPropertyForType("node");
    }

    @Test
    public void addRemoveAutoIndexedPropertyForRelationships() throws JsonParseException {
        addRemoveAutoIndexedPropertyForType("relationship");
    }

    private String relationshipAutoIndexUri()
    {
        return getDataUri() + "index/auto/relationship/";
    }
    
    private void addRemoveAutoIndexedPropertyForType(String uriPartForType) throws JsonParseException {
//      List<String> properties = getAutoIndexedPropertiesForType(uriPartForType);
//      assertTrue(properties.isEmpty());

        gen.get()
                .noGraph()
                .expectedStatus( 204 )
                .payload( "myProperty1" )
                .post(autoIndexURI(uriPartForType) + "/properties");
        gen.get()
                .noGraph()
                .expectedStatus( 204 )
                .payload( "myProperty2" )
                .post(autoIndexURI(uriPartForType) + "/properties");

        List<String> properties = getAutoIndexedPropertiesForType(uriPartForType);
        assertEquals(2, properties.size());
        assertTrue(properties.contains("myProperty1"));
        assertTrue(properties.contains("myProperty2"));

        gen.get()
                .noGraph()
                .expectedStatus(204)
                .payload(null)
                .delete(autoIndexURI(uriPartForType)
                        + "/properties/myProperty2");

        properties = getAutoIndexedPropertiesForType(uriPartForType);
        assertEquals(1, properties.size());
        assertTrue(properties.contains("myProperty1"));
    }

    @SuppressWarnings( "unchecked" )
    private List<String> getAutoIndexedPropertiesForType(String uriPartForType)
            throws JsonParseException
    {
        String result = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get(autoIndexURI(uriPartForType) + "/properties").entity();
        return (List<String>) JsonHelper.readJson(result);
    }

    private void switchOnOffAutoIndexingForType(String uriPartForType)
    {
        setEnabledAutoIndexingForType(uriPartForType, true);
        checkAndAssertAutoIndexerIsEnabled(uriPartForType, true);
        setEnabledAutoIndexingForType(uriPartForType, false);
        checkAndAssertAutoIndexerIsEnabled(uriPartForType, false);
    }

    private void setEnabledAutoIndexingForType(String uriPartForType,
            boolean enabled)
    {
        gen.get()
                .noGraph()
                .expectedStatus( 204 )
                .payload( Boolean.toString( enabled ) )
                .put(autoIndexURI(uriPartForType) + "/status");
    }

    private void checkAndAssertAutoIndexerIsEnabled(String uriPartForType,
            boolean enabled)
    {
        String result = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get(autoIndexURI(uriPartForType) + "/status").entity();
        assertEquals(enabled, Boolean.parseBoolean(result));
    }

    private String autoIndexURI(String type)
    {
        return getDataUri()
                + RestfulGraphDatabase.PATH_AUTO_INDEX.replace("{type}", type);
    }
}
