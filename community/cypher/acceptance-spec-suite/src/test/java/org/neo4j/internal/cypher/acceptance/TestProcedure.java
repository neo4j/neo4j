/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_PRUNE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_PRUNE;

public class TestProcedure
{
    @Context
    public GraphDatabaseService db;

    @Procedure("org.neo4j.test")
    @Description("org.neo4j.test")
    public Stream<TestResult> test() throws Exception {
        return db.findNodes( Label.label("Tweet" ) ).stream().map( TestResult::new );
    }

    @Procedure("org.neo4j.testTraversal")
    @Description("org.neo4j.testTraversal")
    public Stream<TestResult> testTraversal() throws Exception {
        TraversalDescription td = db.traversalDescription();
        return db.findNodes( Label.label("Tweet" ) )
                .stream()
                .flatMap( node -> td.traverse( node ).stream().map( path -> new TestResult( path.startNode() ) ) );
    }

    @Procedure("apoc.path.expandConfig")
    @Description("apoc.path.expandConfig(startNode <id>|Node|list, {minLevel,maxLevel,uniqueness,relationshipFilter,labelFilter,uniqueness:'RELATIONSHIP_PATH',bfs:true, filterStartNode:false}) yield path expand from start node following the given relationships from min to max-level adhering to the label filters")
    public Stream<PathResult> expandConfig(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
        return expandConfigPrivate(start, config).map( PathResult::new );
    }

    private Stream<Path> expandConfigPrivate(@Name("start") Object start, @Name("config") Map<String,Object> config) throws Exception {
        List<Node> nodes = startToNodes(start);

        String uniqueness = (String) config.getOrDefault("uniqueness", Uniqueness.RELATIONSHIP_PATH.name());
        String relationshipFilter = (String) config.getOrDefault("relationshipFilter", null);
        String labelFilter = (String) config.getOrDefault("labelFilter", null);
        long minLevel = toLong(config.getOrDefault("minLevel", "-1"));
        long maxLevel = toLong(config.getOrDefault("maxLevel", "-1"));
        boolean bfs = toBoolean(config.getOrDefault("bfs",true));
        boolean filterStartNode = toBoolean(config.getOrDefault("filterStartNode", true));
        long limit = toLong(config.getOrDefault("limit", "-1"));

        return explorePathPrivate(nodes, relationshipFilter, labelFilter, minLevel, maxLevel, bfs,
                getUniqueness(uniqueness), filterStartNode, limit);
    }

    public static Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number)value).longValue();
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean toBoolean(Object value) {
        if ((value == null || value instanceof Number && (((Number) value).longValue()) == 0L || value instanceof String && (value.equals("") || ((String) value).equalsIgnoreCase("false") || ((String) value).equalsIgnoreCase("no")|| ((String) value).equalsIgnoreCase("0"))|| value instanceof Boolean && value.equals(false))) {
            return false;
        }
        return true;
    }

    private Stream<Path> explorePathPrivate(Iterable<Node> startNodes
            , String pathFilter
            , String labelFilter
            , long minLevel
            , long maxLevel, boolean bfs, Uniqueness uniqueness, boolean filterStartNode, long limit) {
        // LabelFilter
        // -|Label|:Label|:Label excluded label list
        // +:Label or :Label include labels

        Traverser traverser = traverse(db.traversalDescription(), startNodes, pathFilter, labelFilter, minLevel, maxLevel, uniqueness,bfs,filterStartNode,limit);
        return traverser.stream();
    }

    public static Traverser traverse(TraversalDescription traversalDescription, Iterable<Node> startNodes, String pathFilter, String labelFilter, long minLevel, long maxLevel, Uniqueness uniqueness, boolean bfs, boolean filterStartNode, long limit) {
        TraversalDescription td = traversalDescription;
        // based on the pathFilter definition now the possible relationships and directions must be shown

        td = bfs ? td.breadthFirst() : td.depthFirst();

        Iterable<Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(pathFilter);

        for (Pair<RelationshipType, Direction> pair: relDirIterable) {
            if (pair.first() == null) {
                td = td.expand( PathExpanderBuilder.allTypes(pair.other()).build());
            } else {
                td = td.relationships(pair.first(), pair.other());
            }
        }

        if (minLevel != -1) td = td.evaluator( Evaluators.fromDepth((int) minLevel));
        if (maxLevel != -1) td = td.evaluator(Evaluators.toDepth((int) maxLevel));

        if (labelFilter != null && !labelFilter.trim().isEmpty()) {
            td = td.evaluator(new LabelEvaluator(labelFilter, filterStartNode, limit, (int) minLevel));
        }

        td = td.uniqueness(uniqueness); // this is how Cypher works !! Uniqueness.RELATIONSHIP_PATH
        // uniqueness should be set as last on the TraversalDescription
        return td.traverse(startNodes);
    }

    @SuppressWarnings("unchecked")
    private List<Node> startToNodes(Object start) throws Exception {
        if (start == null) return Collections.emptyList();
        if (start instanceof Node) {
            return Collections.singletonList((Node) start);
        }
        if (start instanceof Number) {
            return Collections.singletonList(db.getNodeById(((Number) start).longValue()));
        }
        if (start instanceof List) {
            List list = (List) start;
            if (list.isEmpty()) return Collections.emptyList();

            Object first = list.get(0);
            if (first instanceof Node) return (List<Node>)list;
            if (first instanceof Number) {
                List<Node> nodes = new ArrayList<>();
                for (Number n : ((List<Number>)list)) nodes.add(db.getNodeById(n.longValue()));
                return nodes;
            }
        }
        throw new Exception("Unsupported data type for start parameter a Node or an Identifier (long) of a Node must be given!");
    }

    private Uniqueness getUniqueness(String uniqueness) {
        for (Uniqueness u : Uniqueness.values()) {
            if (u.name().equalsIgnoreCase(uniqueness)) return u;
        }
        return Uniqueness.RELATIONSHIP_PATH;
    }

    public static class PathResult {
        public Path path;

        public PathResult(Path path) {
            this.path = path;
        }
    }

    public static class TestResult
    {
        public String value;

        public TestResult( Node node )
        {
            this.value = "NodeWithId"+value;
        }
    }

    public static class LabelEvaluator implements Evaluator
    {
        private Set<String> whitelistLabels;
        private Set<String> blacklistLabels;
        private Set<String> terminationLabels;
        private Set<String> endNodeLabels;
        private Evaluation whitelistAllowedEvaluation;
        private boolean endNodesOnly;
        private boolean filterStartNode;
        private long limit = -1;
        private long minLevel = -1;
        private long resultCount = 0;

        public LabelEvaluator(String labelFilter, boolean filterStartNode, long limit, int minLevel) {
            this.filterStartNode = filterStartNode;
            this.limit = limit;
            this.minLevel = minLevel;
            Map<Character, Set<String>> labelMap = new HashMap<>(4);

            if (labelFilter !=  null && !labelFilter.isEmpty()) {

                // parse the filter
                // split on |
                String[] defs = labelFilter.split("\\|");
                Set<String> labels = null;

                for (String def : defs) {
                    char operator = def.charAt(0);
                    switch (operator) {
                    case '+':
                    case '-':
                    case '/':
                    case '>':
                        labels = labelMap.computeIfAbsent(operator, character -> new HashSet<>());
                        def = def.substring(1);
                    }

                    if (def.startsWith(":")) {
                        def = def.substring(1);
                    }

                    if (!def.isEmpty()) {
                        labels.add(def);
                    }
                }
            }

            whitelistLabels = labelMap.computeIfAbsent('+', character -> Collections.emptySet());
            blacklistLabels = labelMap.computeIfAbsent('-', character -> Collections.emptySet());
            terminationLabels = labelMap.computeIfAbsent('/', character -> Collections.emptySet());
            endNodeLabels = labelMap.computeIfAbsent('>', character -> Collections.emptySet());
            endNodesOnly = !terminationLabels.isEmpty() || !endNodeLabels.isEmpty();
            whitelistAllowedEvaluation = endNodesOnly ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
        }

        @Override
        public Evaluation evaluate(Path path) {
            int depth = path.length();
            Node check = path.endNode();

            // if start node shouldn't be filtered, exclude/include based on if using termination/endnode filter or not
            // minLevel evaluator will separately enforce exclusion if we're below minLevel
            if (depth == 0 && !filterStartNode) {
                return whitelistAllowedEvaluation;
            }

            // below minLevel always exclude; continue if blacklist and whitelist allow it
            if (depth < minLevel) {
                return labelExists(check, blacklistLabels) || !whitelistAllowed(check) ? EXCLUDE_AND_PRUNE : EXCLUDE_AND_CONTINUE;
            }

            // cut off expansion when we reach the limit
            if (limit != -1 && resultCount >= limit) {
                return EXCLUDE_AND_PRUNE;
            }

            Evaluation result = labelExists(check, blacklistLabels) ? EXCLUDE_AND_PRUNE :
                                labelExists(check, terminationLabels) ? filterEndNode(check, true) :
                                labelExists(check, endNodeLabels) ? filterEndNode(check, false) :
                                whitelistAllowed(check) ? whitelistAllowedEvaluation : EXCLUDE_AND_PRUNE;

            return result;
        }

        private boolean labelExists(Node node, Set<String> labels) {
            if (labels.isEmpty()) {
                return false;
            }

            for ( Label lab : node.getLabels() ) {
                if (labels.contains(lab.name())) {
                    return true;
                }
            }
            return false;
        }

        private boolean whitelistAllowed(Node node) {
            return whitelistLabels.isEmpty() || labelExists(node, whitelistLabels);
        }

        private Evaluation filterEndNode(Node node, boolean isTerminationFilter) {
            resultCount++;
            return isTerminationFilter || !whitelistAllowed(node) ? INCLUDE_AND_PRUNE : INCLUDE_AND_CONTINUE;
        }
    }

    public static abstract class RelationshipTypeAndDirections {

        public static final char BACKTICK = '`';

        public static List<Pair<RelationshipType, Direction>> parse(String pathFilter) {
            List<Pair<RelationshipType, Direction>> relsAndDirs = new ArrayList<>();
            if (pathFilter == null) {
                relsAndDirs.add(Pair.of(null, BOTH)); // todo can we remove this?
            } else {
                String[] defs = pathFilter.split("\\|");
                for (String def : defs) {
                    relsAndDirs.add(Pair.of(relationshipTypeFor(def), directionFor(def)));
                }
            }
            return relsAndDirs;
        }

        private static Direction directionFor(String type) {
            if (type.contains("<")) return INCOMING;
            if (type.contains(">")) return OUTGOING;
            return BOTH;
        }

        private static RelationshipType relationshipTypeFor(String name) {
            if (name.indexOf(BACKTICK) > -1) name = name.substring(name.indexOf(BACKTICK)+1,name.lastIndexOf(BACKTICK));
            else {
                name = name.replaceAll("[<>:]", "");
            }
            return name.trim().isEmpty() ? null : RelationshipType.withName(name);
        }
    }
}
