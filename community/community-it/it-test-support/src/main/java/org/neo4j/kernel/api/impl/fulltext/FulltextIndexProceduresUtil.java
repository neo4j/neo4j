package org.neo4j.kernel.api.impl.fulltext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys.ANALYZER;
import static org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys.PROCEDURE_ANALYZER;
import static org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys.PROCEDURE_EVENTUALLY_CONSISTENT;

public class FulltextIndexProceduresUtil
{
    public static final String DB_INDEXES = "CALL db.indexes()";
    public static final String DROP = "CALL db.index.fulltext.drop(\"%s\")";
    public static final String LIST_AVAILABLE_ANALYZERS = "CALL db.index.fulltext.listAvailableAnalyzers()";
    public static final String DB_AWAIT_INDEX = "CALL db.index.fulltext.awaitIndex(\"%s\")";
    public static final String QUERY_NODES = "CALL db.index.fulltext.queryNodes(\"%s\", \"%s\")";
    public static final String QUERY_RELS = "CALL db.index.fulltext.queryRelationships(\"%s\", \"%s\")";
    public static final String AWAIT_REFRESH = "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()";
    public static final String NODE_CREATE = "CALL db.index.fulltext.createNodeIndex(\"%s\", %s, %s )";
    public static final String RELATIONSHIP_CREATE = "CALL db.index.fulltext.createRelationshipIndex(\"%s\", %s, %s)";
    public static final String NODE_CREATE_WITH_CONFIG = "CALL db.index.fulltext.createNodeIndex(\"%s\", %s, %s, %s)";
    public static final String RELATIONSHIP_CREATE_WITH_CONFIG = "CALL db.index.fulltext.createRelationshipIndex(\"%s\", %s, %s, %s)";

    public static String array( String... args )
    {
        return Arrays.stream( args ).map( s -> "\"" + s + "\"" ).collect( Collectors.joining( ", ", "[", "]" ) );
    }

    public static Map<String,Value> asProcedureConfigMap( String analyzer, boolean eventuallyConsistent )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( PROCEDURE_ANALYZER, Values.stringValue( analyzer ) );
        map.put( PROCEDURE_EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
        return map;
    }

    public static Map<String,Value> asConfigMap( String analyzer, boolean eventuallyConsistent )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( ANALYZER, Values.stringValue( analyzer ) );
        map.put( FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
        return map;
    }

    public static String asConfigString( Map<String,Value> configMap )
    {
        StringJoiner joiner = new StringJoiner( ", ", "{", "}" );
        configMap.forEach( ( k, v ) -> joiner.add( k + ": \"" + v.asObject() + "\"" ) );
        return joiner.toString();
    }
}

