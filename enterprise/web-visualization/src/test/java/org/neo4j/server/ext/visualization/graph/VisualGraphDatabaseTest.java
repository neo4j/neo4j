package org.neo4j.server.ext.visualization.graph;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class VisualGraphDatabaseTest
{
    @Test
    public void shouldBeAGraphDatabaseService() {
        VisualGraphDatabase vgraph = new VisualGraphDatabase( null );
        assertThat(vgraph, is(instanceOf( GraphDatabaseService.class)));
    }
}
