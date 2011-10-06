package org.neo4j.visualization.graphviz;

import static org.junit.Assert.*;

import org.junit.Test;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class AsciidocHelperTest
{

    @Test
    public void test()
    {
        String cypher = "start n=node(0) " +
        		"match " +
        		"x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, " +
                "x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, " +
                "x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n " +
                "return n, x";
        
        String snippet = AsciidocHelper.createCypherSnippet( cypher );
        assertTrue(snippet.contains( "n,\n" ));
        }

}
