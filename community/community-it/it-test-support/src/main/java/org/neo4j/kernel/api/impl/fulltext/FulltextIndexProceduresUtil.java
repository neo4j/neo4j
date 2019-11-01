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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.PROCEDURE_ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.PROCEDURE_EVENTUALLY_CONSISTENT;

public final class FulltextIndexProceduresUtil
{
    public static final String DB_INDEXES = "CALL db.indexes()";
    public static final String DROP = "CALL db.index.fulltext.drop(\"%s\")";
    public static final String LIST_AVAILABLE_ANALYZERS = "CALL db.index.fulltext.listAvailableAnalyzers()";
    public static final String DB_AWAIT_INDEX = "CALL db.awaitIndex(\"%s\")";
    public static final String QUERY_NODES = "CALL db.index.fulltext.queryNodes(\"%s\", \"%s\")";
    public static final String QUERY_RELS = "CALL db.index.fulltext.queryRelationships(\"%s\", \"%s\")";
    public static final String AWAIT_REFRESH = "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()";
    public static final String NODE_CREATE = "CALL db.index.fulltext.createNodeIndex(\"%s\", %s, %s )";
    public static final String RELATIONSHIP_CREATE = "CALL db.index.fulltext.createRelationshipIndex(\"%s\", %s, %s)";
    public static final String NODE_CREATE_WITH_CONFIG = "CALL db.index.fulltext.createNodeIndex(\"%s\", %s, %s, %s)";
    public static final String RELATIONSHIP_CREATE_WITH_CONFIG = "CALL db.index.fulltext.createRelationshipIndex(\"%s\", %s, %s, %s)";

    private FulltextIndexProceduresUtil()
    {
    }

    public static String asCypherStringsList( String... args )
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
        return Map.of( ANALYZER, Values.stringValue( analyzer ), FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
    }

    public static String asConfigString( Map<String,Value> configMap )
    {
        StringJoiner joiner = new StringJoiner( ", ", "{", "}" );
        configMap.forEach( ( k, v ) -> joiner.add( k + ": \"" + v.asObject() + "\"" ) );
        return joiner.toString();
    }
}

