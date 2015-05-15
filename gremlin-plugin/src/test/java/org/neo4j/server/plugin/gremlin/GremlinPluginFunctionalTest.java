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
package org.neo4j.server.plugin.gremlin;

import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.JSONPrettifier;
import org.neo4j.server.webadmin.console.GremlinSessionCreator;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.PropType;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class GremlinPluginFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script";
    
    /**
     * Scripts can be sent as URL-encoded In this example, the graph has been
     * autoindexed by Neo4j, so we can look up the name property on nodes.
     */
    @Test
    @Title( "Send a Gremlin Script - URL encoded" )
    @Documented
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void testGremlinPostURLEncoded() throws UnsupportedEncodingException
    {
        data.get();
        String script = "g.idx('node_auto_index')[[name:'I']].out";
        gen().expectedStatus( OK.getStatusCode() ).description(
                formatGroovy( script ) );
        String response = gen().payload(
                "script=" + URLEncoder.encode( script, "UTF-8" ) ).payloadType(
                MediaType.APPLICATION_FORM_URLENCODED_TYPE ).post( ENDPOINT ).entity();
        assertTrue( response.contains( "you" ) );
    }
    
    /**
     * Sample docs
     */
    @Test
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void testIndexIteration() throws UnsupportedEncodingException
    {
        data.get();
        String script = "g.idx('node_auto_index')[[name:'I']]";
        gen().expectedStatus( OK.getStatusCode() ).description(
                formatGroovy( script ) );
        String response = doRestCall( script, OK);
        assertTrue( response.contains( "I" ) );
    }

    /**
     * Send a Gremlin Script, URL-encoded with UTF-8 encoding, with additional
     * parameters.
     */
    @Title( "Send a Gremlin Script with variables in a JSON Map - URL encoded" )
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostWithVariablesURLEncoded()
            throws UnsupportedEncodingException
    {
        final String script = "g.v(me).out;";
        final String params = "{ \"me\" : " + data.get().get( "I" ).getId()
                              + " }";
        gen().description( formatGroovy( script ) );
        String response = gen().expectedStatus( OK.getStatusCode() ).payload(
                "script=" + URLEncoder.encode( script, "UTF-8" ) + "&params="
                        + URLEncoder.encode( params, "UTF-8" ) )

        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE ).post(
                ENDPOINT ).entity();
        assertTrue( response.contains( "you" ) );
    }

    /**
     * Send a Gremlin Script, as JSON payload and additional parameters
     */
    @Test
    @Title( "Send a Gremlin Script with variables in a JSON Map" )
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostWithVariablesAsJson()
            throws UnsupportedEncodingException
    {
        String response = doRestCall( "g.v(me).out", OK,
                Pair.of( "me", data.get().get( "I" ).getId() + "" ) );
        assertTrue( response.contains( "you" ) );
    }

    private String doRestCall( String script, Status status,
            Pair<String, String>... params )
    {
           return doGremlinRestCall(ENDPOINT, script, status, params);
    }

    protected String doGremlinRestCall( String endpoint, String scriptTemplate, Status status, Pair<String, String>... params ) {
        data.get();
        String parameterString = createParameterString( params );


        String script = createScript( scriptTemplate );
        String queryString = "{\"script\": \"" + script + "\"," + parameterString+"},"  ;

        gen().expectedStatus( status.getStatusCode() ).payload(
                queryString ).description(formatGroovy( script ) );
        return gen().post( endpoint ).entity();
    }





    /**
     * Importing a graph from a http://graphml.graphdrawing.org/[GraphML] file can
     * be achieved through the Gremlin GraphMLReader. The following script
     * imports a small GraphML file from an URL into Neo4j, resulting in the
     * depicted graph. The underlying database is auto-indexed, see <<auto-indexing>>
     * so the script can return the imported node by index lookup.
     */
    @Test
    @Documented
    @Graph( value = {"Peter is Test"},autoIndexNodes=true, autoIndexRelationships=true )
    @Title( "Load a sample graph" )
    public void testGremlinImportGraph() throws UnsupportedEncodingException
    {
        data.get().clear();

        URL graphML = GremlinPluginFunctionalTest.class.getResource( "/graphml.xml" );

        String script = "" +
        		"g.clear();" +
        		"g.loadGraphML('" + graphML + "');" +
        		"g.idx('node_auto_index')[[name:'you']];";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "you" ) );
    }
    
    
    
    @Test
    public void return_map() throws UnsupportedEncodingException
    {
        String script = "m = [name:'John',age:24, address:[number:34]];";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "\"number\" : 34" ) );
    }
    /**
     * Exporting a graph can be done by simple emitting the appropriate String.
     */
    @Test
    @Documented
    @Title( "Emit a sample graph" )
    @Graph( value = { "I know you", "I know him" } )
    public void emitGraph() throws UnsupportedEncodingException
    {
        String script = "writer = new GraphMLWriter(g);"
                        + "out = new java.io.ByteArrayOutputStream();"
                        + "writer.outputGraph(out);"
                        + "result = out.toString();";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "graphml" ) );
        assertTrue( response.contains( "you" ) );
    }

    /**
     * To set variables in the bindings for the Gremlin Script Engine on the
     * server, you can include a +params+ parameter with a String representing a
     * JSON map of variables to set to initial values. These can then be
     * accessed as normal variables within the script.
     */
    @Test
    @Documented
    @Title( "Set script variables" )
    public void setVariables() throws UnsupportedEncodingException
    {
        String script = "meaning_of_life";
        String payload = "{\"script\":\"" + script + "\","
                         + "\"params\":{\"meaning_of_life\" : 42.0}}";
        description( formatGroovy( script ) );
        String response = gen().expectedStatus( OK.getStatusCode() ).payload(
                JSONPrettifier.parse( payload ) ).payloadType(
                MediaType.APPLICATION_JSON_TYPE ).post( ENDPOINT ).entity();
        assertTrue( response.contains( "42.0" ) );
    }

    /**
     * The following script returns a sorted list of all nodes connected via
     * outgoing relationships to node 1, sorted by their `name`-property.
     */
    @Test
    @Documented
    @Title( "Sort a result using raw Groovy operations" )
    @Graph( value = { "I know you", "I know him" }, autoIndexNodes = true )
    public void testSortResults() throws UnsupportedEncodingException
    {
        data.get();
        String script = "g.idx('node_auto_index')[[name:'I']].out.sort{it.name}";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "you" ) );
        assertTrue( response.contains( "him" ) );
        assertTrue( response.indexOf( "you" ) > response.indexOf( "him" ) );
    }

    /**
     * The following script returns paths. Paths in Gremlin consist of the pipes
     * that make up the path from the starting pipes. The server is returning
     * JSON representations of their content as a nested list.
     */
    @Test
    @Title( "Return paths from a Gremlin script" )
    @Documented
    @Graph( value = { "I know you", "I know him" } )
    public void testScriptWithPaths()
    {
        String script = "g.v(%I%).out.name.paths";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "you" ) );
        assertTrue( response.contains( "him" ) );
    }

    @Test
    public void testLineBreaks() throws UnsupportedEncodingException
    {
        // be aware that the string is parsed in Java before hitting the wire,
        // so escape the backslash once in order to get \n on the wire.
        String script = "1\\n2";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "2" ) );
    }

    /**
     * To send a Script JSON encoded, set the payload Content-Type Header. In
     * this example, find all the things that my friends like, and return a
     * table listing my friends by their name, and the names of the things they
     * like in a table with two columns, ignoring the third named step variable
     * +I+. Remember that everything in Gremlin is an iterator - in order to
     * populate the result table +t+, iterate through the pipes with
     * +iterate()+.
     */
    @Test
    @Title( "Send a Gremlin Script - JSON encoded with table results" )
    @Documented
    @Graph( value = { "I know Joe", "I like cats", "Joe like cats",
            "Joe like dogs" } )
    public void testGremlinPostJSONWithTableResult()
    {
        String script = "t= new Table();"
                        + "g.v(%I%).as('I').out('know').as('friend').out('like').as('likes').table(t,['friend','likes']){it.name}{it.name}.iterate();t;";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "cats" ) );
    }

    /**
     * When returning an iterable nested result like a pipe, the data will be
     * serialized according to the recursive resolution of the pipe elements, as
     * shown below.
     */
    @Test
    @Graph( value = { "I know Joe", "I like cats", "Joe like cats",
            "Joe like dogs" } )
    public void returning_nested_pipes()
    {
        String script = "g.v(%I%).as('I').out('know').as('friend').out('like').as('likes').table(new Table()){it.name}{it.name}.cap;";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "cats" ) );
    }

    /**
     * Send an arbitrary Groovy script - Lucene sorting.
     * 
     * This example demonstrates that you via the Groovy runtime embedded with
     * the server have full access to all of the servers Java APIs. The below
     * example creates Nodes in the database both via the Blueprints and the
     * Neo4j API indexes the nodes via the native Neo4j Indexing API constructs
     * a custom Lucene sorting and searching returns a Neo4j IndexHits result
     * iterator.
     */
    @Test
    @Documented
    @Graph( value = {  } )
    public void sendArbtiraryGroovy()
    {
        String script = ""

                        + "'******** Additional imports *********';"
                        + "import org.neo4j.graphdb.index.*;"
                        + "import org.neo4j.graphdb.*;"
                        + "import org.neo4j.index.lucene.*;"
                        + "import org.apache.lucene.search.*;"
                        + ";"
                        + "'**** Blueprints API methods on the injected Neo4jGraph at variable g ****';"
                        + "meVertex = g.addVertex([name:'me']);"
                        + "meNode = meVertex.getRawVertex();"
                        + ";"
                        + "'*** get the Neo4j raw instance ***';"
                        + "neo4j = g.getRawGraph();"
                        + ";"
                        + ";"
                        + "'******** Neo4j API methods: *********';"
                        + "tx = neo4j.beginTx();"
                        + " youNode = neo4j.createNode();"
                        + " youNode.setProperty('name','you');"
                        + " youNode.createRelationshipTo(meNode,DynamicRelationshipType.withName('knows'));"
                        + ";"
                        + "'*** index using Neo4j APIs ***';"
                        + " idxManager = neo4j.index();"
                        + " personIndex = idxManager.forNodes('persons');"
                        + " personIndex.add(meNode,'name',meNode.getProperty('name'));"
                        + " personIndex.add(youNode,'name',youNode.getProperty('name'));"
                        + "tx.success();"
                        + "tx.finish();"
                        + ";"
                        + ";"
                        + "'*** Prepare a custom Lucene query context with Neo4j API ***';"
                        + "query = new QueryContext( 'name:*' ).sort( new Sort(new SortField( 'name',SortField.STRING, true ) ) );"
                        + "results = personIndex.query( query );";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "me" ) );

    }

    /**
     * Imagine a user being part of different groups. A group can have different
     * roles, and a user can be part of different groups. He also can have
     * different roles in different groups apart from the membership. The
     * association of a User, a Group and a Role can be referred to as a
     * _HyperEdge_. However, it can be easily modeled in a property graph as a
     * node that captures this n-ary relationship, as depicted below in the
     * +U1G2R1+ node.
     * 
     * To find out in what roles a user is for a particular groups (here
     * 'Group2'), the following script can traverse this HyperEdge node and
     * provide answers.
     */
    @Test
    @Title( "HyperEdges - find user roles in groups" )
    @Documented
    @Graph( value = { "User1 in Group1", "User1 in Group2",
            "Group2 canHave Role2", "Group2 canHave Role1",
            "Group1 canHave Role1", "Group1 canHave Role2", "Group1 isA Group",
            "Group2 isA Group", "Role1 isA Role", "Role2 isA Role",
            "User1 hasRoleInGroup U1G2R1", "U1G2R1 hasRole Role1",
            "U1G2R1 hasGroup Group2", "User1 hasRoleInGroup U1G1R2",
            "U1G1R2 hasRole Role2", "U1G1R2 hasGroup Group1" } )
    public void findGroups()
    {
        String script = "" + "g.v(%User1%)"
                        + ".out('hasRoleInGroup').as('hyperedge')."
                        + "out('hasGroup').filter{it.name=='Group2'}."
                        + "back('hyperedge').out('hasRole').name";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "Role1" ) );
        assertFalse( response.contains( "Role2" ) );

    }

    /**
     * This example is showing a group count in Gremlin, for instance the
     * counting of the different relationship types connected to some the start
     * node. The result is collected into a variable that then is returned.
     */
    @Test
    @Documented
    @Graph( { "Peter knows Ian", "Ian knows Peter", "Peter likes Bikes" } )
    public void group_count() throws UnsupportedEncodingException, Exception
    {
        String script = "m = [:];"
                        + "g.v(%Peter%).bothE().label.groupCount(m).iterate();m";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "\"knows\" : 2" ) );
    }

    /**
     * This example shows how to modify the graph while traversing it. In this
     * case, the Peter node is disconnected from all other nodes.
     * 
     * @@graph1
     */
    @Test
    @Documented
    @Graph( { "Peter knows Ian", "Ian knows Peter", "Peter likes Bikes" } )
    public void modify_the_graph_while_traversing()
            throws UnsupportedEncodingException, Exception
    {
        data.get();
        gen().addSnippet(
                "graph1",
                AsciidocHelper.createGraphViz( "Starting Graph", graphdb(),
                        "starting_graph" + gen.get().getTitle() ) );
        assertTrue( getNode( "Peter" ).hasRelationship() );
        String script = "g.v(%Peter%).bothE.each{g.removeEdge(it)}";
        String response = doRestCall( script, OK );
        assertFalse( getNode( "Peter" ).hasRelationship() );
    }

    /**
     * Multiple traversals can be combined into a single result, using splitting
     * and merging pipes in a lazy fashion.
     */
    @Test
    @Documented
    @Graph( value = { "Peter knows Ian", "Ian knows Peter", "Marie likes Peter" }, autoIndexNodes = true )
    public void collect_multiple_traversal_results()
            throws UnsupportedEncodingException, Exception
    {
        String script = "g.idx('node_auto_index')[[name:'Peter']].copySplit(_().out('knows'), _().in('likes')).fairMerge.name";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "Marie" ) );
        assertTrue( response.contains( "Ian" ) );
    }

    @Test
    public void getExtension()
    {
        String entity = gen.get().expectedStatus( OK.getStatusCode() ).get(
                ENDPOINT ).entity();
        assertTrue( entity.contains( "map" ) );

    }

    /**
     * In order to return only certain sections of a Gremlin result, you can use
     * native Groovy contstructs like +drop()+ and +take()+ to skip and chunk
     * the result set. However, these are not lazy and will build up memory. It
     * is better to use the Gremlin +[start..end]+ pipe instead, providing a
     * lazy way for doing this.
     * 
     * Also, note the use of the +filter{}+ closure to filter nodes.
     */
    @Test
    @Graph( value = { "George knows Sara", "George knows Ian" }, autoIndexNodes = true )
    public void chunking_and_offsetting_in_Gremlin()
            throws UnsupportedEncodingException
    {
        String script = "g.v(%George%).out('knows').filter{it.name == 'Sara'}[0..100]";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "Sara" ) );
        assertFalse( response.contains( "Ian" ) );
    }

    /**
     * Of course, Neo4j primitives liek +Nodes+, +Relationships+ and
     * +GraphDatabaseService+ are returned as Neo4j REST entities
     */
    @Test
    @Graph( value = { "George knows Sara", "George knows Ian" }, autoIndexNodes = true )
    public void returning_Neo4j_primitives()
            throws UnsupportedEncodingException
    {
        String script = "g.getRawGraph()";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "neo4j_version" ) );
    }

    /**
     * 
     */
    @Test
    @Graph( value = { "George knows Sara", "George knows Ian" }, autoIndexNodes = true )
    public void returning_paths() throws UnsupportedEncodingException
    {
        String script = "g.v(%George%).out().paths()";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "Ian" ) );
        assertTrue( response.contains( "Sara" ) );
    }

    /**
     * This example demonstrates basic collaborative filtering - ordering a
     * traversal after occurence counts and substracting objects that are not
     * interesting in the final result.
     * 
     * Here, we are finding Friends-of-Friends that are not Joes friends
     * already. The same can be applied to graphs of users that +LIKE+ things
     * and others.
     */
    @Documented
    @Test
    @Graph( value = { "Joe knows Bill", "Joe knows Sara", "Sara knows Bill",
            "Sara knows Ian", "Bill knows Derrick", "Bill knows Ian",
            "Sara knows Jill" }, autoIndexNodes = true )
    public void collaborative_filtering() throws UnsupportedEncodingException
    {
        String script = "x=[];fof=[:];"
                        + "g.v(%Joe%).out('knows').aggregate(x).out('knows').except(x).groupCount(fof).iterate();fof.sort{a,b -> b.value <=> a.value}";
        String response = doRestCall( script, OK );
        assertFalse( response.contains( "v[" + data.get().get( "Bill" ).getId() ) );
        assertFalse( response.contains( "v[" + data.get().get( "Sara" ).getId() ) );
        assertTrue( response.contains( "v[" + data.get().get( "Ian" ).getId() ) );
        assertTrue( response.contains( "v[" + data.get().get( "Jill" ).getId() ) );
        assertTrue( response.contains( "v["
                                       + data.get().get( "Derrick" ).getId() ) );
    }
    
    /**
     */
    @Documented
    @Test
    @Graph( value = { 
            "Root AllFriends John", 
            "Root AllFriends Jack", 
            "Root AllFriends Jill", 
            "John HasPet ScoobieDoo",
            "Jack HasPet Garfield",
            "ScoobieDoo HasCareTaker Bob",
            "Garfield HasCareTaker Harry" }
    , autoIndexNodes = true )
    public void table_projections() throws UnsupportedEncodingException
    {
        data.get();
        String script = "g.v(%Root%).out('AllFriends').as('Friend').ifThenElse" +
        		"{it.out('HasPet').hasNext()}" +
        		"{it.out('HasPet')}" +
        		"{it}" +
        		".as('Pet').out('HasCareTaker').as('CareTaker').table(new Table()){it['name']}{it['name']}{it['name']}.cap";
        String response = doRestCall( script, OK );
        System.out.println(response);
    }

    /**
     * This is a basic stub example for implementing flow algorithms in for
     * instance http://en.wikipedia.org/wiki/Flow_network[Flow Networks] with a
     * pipes-based approach and scripting, here between +source+ and +sink+
     * using the +capacity+ property on relationships as the base for the flow
     * function and modifying the graph during calculation.
     * 
     * @@graph1
     */
    @Documented
    @Test
    @Graph( nodes = { @NODE( name = "source", setNameProperty = true ),
            @NODE( name = "middle", setNameProperty = true ),
            @NODE( name = "sink", setNameProperty = true ) }, relationships = {
            @REL( start = "source", end = "middle", type = "CONNECTED", properties = { @PROP( key = "capacity", value = "1", type = PropType.INTEGER ) } ),
            @REL( start = "middle", end = "sink", type = "CONNECTED", properties = { @PROP( key = "capacity", value = "3", type = PropType.INTEGER ) } ),
            @REL( start = "source", end = "sink", type = "CONNECTED", properties = { @PROP( key = "capacity", value = "1", type = PropType.INTEGER ) } ),
            @REL( start = "source", end = "sink", type = "CONNECTED", properties = { @PROP( key = "capacity", value = "2", type = PropType.INTEGER ) } ) }, autoIndexNodes = true )
    public void flow_algorithms_with_Gremlin()
            throws UnsupportedEncodingException
    {
        data.get();
        gen().addSnippet(
                "graph1",
                AsciidocHelper.createGraphViz( "Starting Graph", graphdb(),
                        "starting_graph" + gen.get().getTitle() ) );
        String script = "source=g.v(%source%);sink=g.v(%sink%);maxFlow = 0;"
                        + "source.outE.inV.loop(2){!it.object.equals(sink)}.paths.each{"
                        + "flow = it.capacity.min(); "
                        + "maxFlow += flow;"
                        + "it.findAll{it.capacity}.each{it.capacity -= flow}};maxFlow";
        String response = doRestCall( script, OK );
        assertTrue( response.contains( "4" ) );
    }

    @Test
    @Ignore
    @Graph( value = { "Peter knows Ian", "Ian knows Peter", "Marie likes Peter" }, autoIndexNodes = true, autoIndexRelationships = true )
    public void test_Gremlin_load()
    {
        data.get();
        String script = "nodeIndex = g.idx('node_auto_index');"
                        + "edgeIndex = g.idx('relationship_auto_index');"
                        + ""
                        + "node = { uri, properties -> "
                        + "existing = nodeIndex.get('uri', uri);"
                        + "properties['uri'] = uri;"
                        + "if (existing) {    "
                        + "return existing[0];  "
                        + "}  else {"
                        + "    return g.addVertex(properties);"
                        + "};"
                        + "};"
                        + "Object.metaClass.makeNode = node;"
                        + "edge = { type, source_uri, target_uri, properties ->"
                        + "  source = nodeIndex.get('uri', source_uri).iterate();"
                        + "  target = nodeIndex.get('uri', target_uri).iterate();"
                        + "  nodeKey = source.id + '-' + target.id;"
                        + "  existing = edgeIndex.get('nodes', nodeKey);"
                        + "  if (existing) {" + "    return existing;" + "  };"
                        + "  properties['nodes'] = nodeKey;"
                        + "  g.addEdge(source, target, type, properties);"
                        + "};" + "Object.metaClass.makeEdge = edge;";
        String payload = "{\"script\":\"" + script + "\"}";
        description( formatGroovy( script ) );
        gen.get().expectedStatus( OK.getStatusCode() ).payload(
                JSONPrettifier.parse( payload ) );
        String response = gen.get().post( ENDPOINT ).entity();
        for ( int i = 0; i < 1000; i++ )
        {
            String uri = "uri" + i;
            payload = "{\"script\":\"n = Object.metaClass.makeNode('" + uri
                      + "',[:]\"}";
            gen.get().expectedStatus( OK.getStatusCode() ).payload(
                    JSONPrettifier.parse( payload ) );
            response = gen.get().post( ENDPOINT ).entity();
            assertTrue( response.contains( uri ) );
        }
        for ( int i = 0; i < 999; i++ )
        {
            String uri = "uri";
            payload = "{\"script\":\"n = Object.metaClass.makeEdge('knows','"
                      + uri + i + "','" + uri + ( i + 1 ) + "'[:]\"}";
            gen.get().expectedStatus( OK.getStatusCode() ).payload(
                    JSONPrettifier.parse( payload ) );
            response = gen.get().post( ENDPOINT ).entity();
            assertTrue( response.contains( uri ) );
        }
    }

    /**
     * A simple query returning all nodes connected to node 1, returning the
     * node and the name property, if it exists, otherwise `null`:
     */
    @Test
    @Documented
    @Title( "Send a Query" )
    @Graph( "I know you" )
    public void testMixedAccessPatterns() throws UnsupportedEncodingException {
        data.get();
        String response = gen().expectedStatus(OK.getStatusCode()).payload("{\"command\":\"g.clear();g.addVertex([name:'foo']);\",\"engine\":\"gremlin\"}").post("http://localhost:7474/db/manage/server/console/").entity();
        response = gen().expectedStatus(OK.getStatusCode()).payload("{\"script\":\"g.clear()\"}").post(ENDPOINT).entity();
        //response = gen().expectedStatus(OK.getStatusCode()).payload("{\"command\":\"init()\",\"engine\":\"gremlin\"}").post("http://localhost:7474/db/manage/server/console/").entity();
        response = gen().expectedStatus(OK.getStatusCode()).payload("{\"command\":\"g.addVertex([name:'foo'])\",\"engine\":\"gremlin\"}").post("http://localhost:7474/db/manage/server/console/").entity();

    }
    
    
    /**
     * Script errors
     * will result in an HTTP error response code.
     */
    @Test
    @Documented
    @Graph( "I know you" )
    public void script_execution_errors() throws UnsupportedEncodingException {
        data.get();
        String response = gen().expectedStatus(Status.BAD_REQUEST.getStatusCode()).
                payload("{\"script\":\"g.addVertex([name:{}])\"}").post(ENDPOINT).entity();
        assertTrue( response.contains( "BadInputException" ) );
    }

       
    
    
    protected String formatGroovy( String script )
    {
        script = script.replace( ";", "\n" );
        if ( !script.endsWith( "\n" ) )
        {
            script += "\n";
        }
        return "_Raw script source_\n\n" + "[source, groovy]\n" + "----\n"
               + script + "----\n";
    }
    
    @BeforeClass
    public static void addGremlinShellConfig() {
        Configurator.DEFAULT_MANAGEMENT_CONSOLE_ENGINES.add(new GremlinSessionCreator().name());
    }
    
    @AfterClass
    public static void removeGremlinShellConfig() {
        Configurator.DEFAULT_MANAGEMENT_CONSOLE_ENGINES.remove(new GremlinSessionCreator().name());
    }
}
