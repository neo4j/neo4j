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
package org.neo4j.kernel.impl.api.integrationtest;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;

import static org.hamcrest.Matchers.startsWith;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;

public interface ProcedureITBase
{
    default List<Object[]> getExpectedCommunityProcs()
    {
        return List.of(
                proc( "dbms.listConfig", "(searchString =  :: STRING?) :: (name :: STRING?, description :: STRING?, value :: STRING?, dynamic :: BOOLEAN?)",
                        "List the currently active config of Neo4j.", "DBMS" ),
                proc( "db.constraints", "() :: (name :: STRING?, description :: STRING?)", "List all constraints in the database.", "READ" ),
                proc( "db.indexes",
                        "() :: (id :: INTEGER?, name :: STRING?, state :: STRING?, populationPercent :: FLOAT?, uniqueness :: STRING?, type :: STRING?, " +
                                "entityType :: STRING?, labelsOrTypes :: LIST? OF STRING?, properties :: LIST? OF STRING?, provider :: STRING?)",
                        "List all indexes in the database.", "READ" ),
                proc( "db.indexDetails",
                        "(indexName :: STRING?) :: (id :: INTEGER?, name :: STRING?, state :: STRING?, populationPercent :: FLOAT?, uniqueness :: STRING?, " +
                                "type :: STRING?, entityType :: STRING?, labelsOrTypes :: LIST? OF STRING?, properties :: LIST? OF STRING?, provider :: " +
                                "STRING?, indexConfig :: MAP?, failureMessage :: STRING?)", "Detailed description of specific index.", "READ" ),
                proc( "db.awaitIndex", "(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\")).", "READ" ),
                proc( "db.awaitIndexes", "(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\")).", "READ" ),
                proc( "db.resampleIndex", "(index :: STRING?) :: VOID",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\")).", "READ" ),
                proc( "db.resampleOutdatedIndexes", "() :: VOID", "Schedule resampling of all outdated indexes.", "READ" ),
                proc( "db.propertyKeys", "() :: (propertyKey :: STRING?)", "List all property keys in the database.", "READ" ),
                proc( "db.labels", "() :: (label :: STRING?, nodeCount :: INTEGER?)", "List all labels in the database and their total count.", "READ" ),
                proc( "db.schema.visualization", "() :: (nodes :: LIST? OF NODE?, relationships :: LIST? OF RELATIONSHIP?)",
                        "Visualize the schema of the data.", "READ" ), proc( "db.schema.nodeTypeProperties",
                        "() :: (nodeType :: STRING?, nodeLabels :: LIST? OF STRING?, propertyName :: STRING?, " +
                                "propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)", "Show the derived property schema of the nodes in tabular form.",
                        "READ" ), proc( "db.schema.relTypeProperties",
                        "() :: (relType :: STRING?, " + "propertyName :: STRING?, propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the relationships in tabular form.", "READ" ),
                proc( "db.relationshipTypes", "() :: (relationshipType :: STRING?, relationshipCount :: INTEGER?)",
                        "List all relationship types in the database and their total count.", "READ" ), proc( "dbms.procedures",
                        "() :: (name :: STRING?, signature :: " + "STRING?, description :: STRING?, mode :: STRING?, worksOnSystem :: BOOLEAN?)",
                        "List all procedures in the DBMS.", "DBMS" ),
                proc( "dbms.functions", "() :: (name :: STRING?, signature :: " + "STRING?, description :: STRING?, aggregating :: BOOLEAN?)",
                        "List all functions in the DBMS.", "DBMS" ),
                proc( "dbms.components", "() :: (name :: STRING?, versions :: LIST? OF" + " STRING?, edition :: STRING?)",
                        "List DBMS components and their versions.", "DBMS" ),
                proc( "dbms.queryJmx", "(query :: STRING?) :: (name :: STRING?, " + "description :: STRING?, attributes :: MAP?)",
                        "Query JMX management data by domain and name." + " For instance, \"org.neo4j:*\"", "DBMS" ),
                proc( "db.createLabel", "(newLabel :: STRING?) :: VOID", "Create a label", "WRITE", false ),
                proc( "db.createProperty", "(newProperty :: STRING?) :: VOID", "Create a Property", "WRITE", false ),
                proc( "db.createRelationshipType", "(newRelationshipType :: STRING?) :: VOID", "Create a RelationshipType", "WRITE", false ),
                proc( "dbms.clearQueryCaches", "() :: (value :: STRING?)", "Clears all query caches.", "DBMS" ),
                proc( "db.createIndex", "(index :: STRING?, providerName :: STRING?) :: (index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a schema index with specified index provider (for example: CALL db.createIndex(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA", false ), proc( "db.createUniquePropertyConstraint",
                        "(index :: STRING?, providerName :: STRING?) :: " + "(index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a unique property constraint with index backed by specified index provider " +
                                "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA", false ),
                proc( "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", "() :: VOID",
                        "Wait for the updates from recently committed transactions to be applied to any eventually-consistent fulltext indexes.", "READ" ),
                proc( "db.index.fulltext.awaitIndex", "(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Similar to db.awaitIndex(index, timeout), except instead of an index pattern, the index is specified by name. " +
                                "The name can be quoted by backticks, if necessary.", "READ" ), proc( "db.index.fulltext.createNodeIndex",
                        "(indexName :: STRING?, labels :: LIST? OF STRING?, propertyNames :: LIST? OF STRING?, " + "config = {} :: MAP?) :: VOID",
                        startsWith( "Create a node fulltext index for the given labels and properties." ), "SCHEMA", false ),
                proc( "db.index.fulltext.createRelationshipIndex",
                        "(indexName :: STRING?, relationshipTypes :: LIST? OF STRING?, propertyNames :: LIST? OF STRING?, config = {} :: MAP?) :: VOID",
                        startsWith( "Create a relationship fulltext index for the given relationship types and properties." ), "SCHEMA", false ),
                proc( "db.index.fulltext.drop", "(indexName :: STRING?) :: VOID", "Drop the specified index.", "SCHEMA", false ),
                proc( "db.index.fulltext.listAvailableAnalyzers", "() :: (analyzer :: STRING?, description :: STRING?)",
                        "List the available analyzers that the fulltext indexes can be configured with.", "READ" ),
                proc( "db.index.fulltext.queryNodes", "(indexName :: STRING?, queryString :: STRING?) :: (node :: NODE?, score :: FLOAT?)",
                        "Query the given fulltext index. Returns the matching nodes and their lucene query score, ordered by score.", "READ" ),
                proc( "db.index.fulltext.queryRelationships",
                        "(indexName :: STRING?, queryString :: STRING?) :: (relationship :: RELATIONSHIP?, " + "score :: FLOAT?)",
                        "Query the given fulltext index. Returns the matching relationships and their lucene query score, ordered by " + "score.", "READ" ),
                proc( "db.prepareForReplanning", "(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Triggers an index resample and waits for it to complete, and after that clears query caches." +
                                " After this procedure has finished queries will be planned using the latest database " + "statistics.", "READ" ),
                proc( "db.stats.retrieve", "(section :: STRING?, config = {} :: MAP?) :: (section :: STRING?, data :: MAP?)",
                        "Retrieve statistical data about the current database. Valid sections are 'GRAPH COUNTS', 'TOKENS', 'QUERIES', 'META'", "READ" ),
                proc( "db.stats.retrieveAllAnonymized", "(graphToken :: STRING?, config = {} :: MAP?) :: (section :: STRING?, data :: MAP?)",
                        "Retrieve all available statistical data about the current database, in an anonymized form.", "READ" ),
                proc( "db.stats.status", "() :: (section :: STRING?, status :: STRING?, data :: MAP?)",
                        "Retrieve the status of all available collector daemons, for this database.", "READ" ),
                proc( "db.stats.collect", "(section :: STRING?, config = {} :: MAP?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                        "Start data collection of a given data section. Valid sections are 'QUERIES'", "READ" ),
                proc( "db.stats.stop", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                        "Stop data collection of a given data section. Valid sections are 'QUERIES'", "READ" ),
                proc( "db.stats.clear", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                        "Clear collected data of a given data section. Valid sections are 'QUERIES'", "READ" ),
                proc( "dbms.routing.getRoutingTable", "(context :: MAP?, database = null :: STRING?) :: (ttl :: INTEGER?, servers :: LIST? OF MAP?)",
                        "Returns endpoints of this instance.", "DBMS" ),
                proc( "dbms.cluster.routing.getRoutingTable", "(context :: MAP?, database = null :: STRING?) :: (ttl :: INTEGER?, servers :: LIST? OF MAP?)",
                        "Returns endpoints of this instance.", "DBMS" ),
                proc( "dbms.setTXMetaData", "(data :: MAP?) :: VOID",
                        "Attaches a map of data to the transaction. The data will be printed when listing queries, and inserted into the query log.", "DBMS", false),
                proc( "dbms.getTXMetaData", "() :: (metadata :: MAP?)", "Provides attached transaction metadata.", "DBMS" ));
    }

    default List<Object[]> getExpectedEnterpriseProcs()
    {
        // Wouldn't it be nice to just grab the community one and add a few procedures?
        // Well, we cannot do this because enterprise has a different signature ( a roles column added)
        //TODO: once both are the same, we can remove code duplication here
        return List.of(
                proc( "dbms.listConfig", "(searchString =  :: STRING?) :: (name :: STRING?, description :: STRING?, value :: STRING?, dynamic :: BOOLEAN?)",
                        "List the currently active config of Neo4j.", stringArray( "admin" ), "DBMS" ),
                proc( "db.constraints", "() :: (name :: STRING?, description :: STRING?)", "List all constraints in the database.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.indexes",
                        "() :: (id :: INTEGER?, name :: STRING?, state :: STRING?, populationPercent :: FLOAT?, uniqueness :: STRING?, type :: STRING?, " +
                                "entityType :: STRING?, labelsOrTypes :: LIST? OF STRING?, properties :: LIST? OF STRING?, provider :: STRING?)",
                        "List all indexes in the database.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.indexDetails",
                        "(indexName :: STRING?) :: (id :: INTEGER?, name :: STRING?, state :: STRING?, populationPercent :: FLOAT?, uniqueness :: STRING?, " +
                                "type :: STRING?, entityType :: STRING?, labelsOrTypes :: LIST? OF STRING?, properties :: LIST? OF STRING?, provider :: " +
                                "STRING?, indexConfig :: MAP?, failureMessage :: STRING?)", "Detailed description of specific index.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.awaitIndex", "(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\")).",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.awaitIndexes", "(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\")).",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ), proc( "db.resampleIndex", "(index :: STRING?) :: VOID",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\")).",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.resampleOutdatedIndexes", "() :: VOID", "Schedule resampling of all outdated indexes.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.propertyKeys", "() :: (propertyKey :: STRING?)", "List all property keys in the database.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.labels", "() :: (label :: STRING?, nodeCount :: INTEGER?)", "List all labels in the database and their total count.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.schema.visualization", "() :: (nodes :: LIST? OF NODE?, relationships :: LIST? OF RELATIONSHIP?)",
                        "Visualize the schema of the data.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.schema.nodeTypeProperties", "() :: (nodeType :: STRING?, nodeLabels :: LIST? OF STRING?, propertyName :: STRING?, " +
                                "propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)", "Show the derived property schema of the nodes in tabular form.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ), proc( "db.schema.relTypeProperties",
                        "() :: (relType :: STRING?, " + "propertyName :: STRING?, propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the relationships in tabular form.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.relationshipTypes", "() :: (relationshipType :: STRING?, relationshipCount :: INTEGER?)",
                        "List all relationship types in the database and their total count.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ), proc( "dbms.procedures",
                        "() :: (name :: STRING?, signature :: " + "STRING?, description :: STRING?, mode :: STRING?, worksOnSystem :: BOOLEAN?)",
                        "List all procedures in the DBMS.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.functions", "() :: (name :: STRING?, signature :: " + "STRING?, description :: STRING?, aggregating :: BOOLEAN?)",
                        "List all functions in the DBMS.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.components", "() :: (name :: STRING?, versions :: LIST? OF" + " STRING?, edition :: STRING?)",
                        "List DBMS components and their versions.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.queryJmx", "(query :: STRING?) :: (name :: STRING?, " + "description :: STRING?, attributes :: MAP?)",
                        "Query JMX management data by domain and name." + " For instance, \"org.neo4j:*\"",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "db.createLabel", "(newLabel :: STRING?) :: VOID", "Create a label", stringArray( "editor", "publisher", "architect", "admin" ), "WRITE",
                        false ),
                proc( "db.createProperty", "(newProperty :: STRING?) :: VOID", "Create a Property", stringArray( "editor", "publisher", "architect", "admin" ),
                        "WRITE", false ), proc( "db.createRelationshipType", "(newRelationshipType :: STRING?) :: VOID", "Create a RelationshipType",
                        stringArray( "editor", "publisher", "architect", "admin" ), "WRITE", false ),
                proc( "dbms.clearQueryCaches", "() :: (value :: STRING?)", "Clears all query caches.", stringArray( "admin" ), "DBMS" ),
                proc( "db.createIndex", "(index :: STRING?, providerName :: STRING?) :: (index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a schema index with specified index provider (for example: CALL db.createIndex(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", stringArray( "architect", "admin" ), "SCHEMA", false ),
                proc( "db.createUniquePropertyConstraint",
                        "(index :: STRING?, providerName :: STRING?) :: " + "(index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a unique property constraint with index backed by specified index provider " +
                                "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", stringArray( "architect", "admin" ), "SCHEMA", false ),
                proc( "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", "() :: VOID",
                        "Wait for the updates from recently committed transactions to be applied to any eventually-consistent fulltext indexes.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.index.fulltext.awaitIndex", "(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Similar to db.awaitIndex(index, timeout), except instead of an index pattern, the index is specified by name. " +
                                "The name can be quoted by backticks, if necessary.", stringArray( "reader", "editor", "publisher", "architect", "admin" ),
                        "READ" ), proc( "db.index.fulltext.createNodeIndex",
                        "(indexName :: STRING?, labels :: LIST? OF STRING?, propertyNames :: LIST? OF STRING?, " + "config = {} :: MAP?) :: VOID",
                        startsWith( "Create a node fulltext index for the given labels and properties." ), stringArray( "architect", "admin" ), "SCHEMA",
                        false ), proc( "db.index.fulltext.createRelationshipIndex",
                        "(indexName :: STRING?, relationshipTypes :: LIST? OF STRING?, propertyNames :: LIST? OF STRING?, config = {} :: MAP?) :: VOID",
                        startsWith( "Create a relationship fulltext index for the given relationship types and properties." ),
                        stringArray( "architect", "admin" ), "SCHEMA", false ),
                proc( "db.index.fulltext.drop", "(indexName :: STRING?) :: VOID", "Drop the specified index.", stringArray( "architect", "admin" ), "SCHEMA",
                        false ), proc( "db.index.fulltext.listAvailableAnalyzers", "() :: (analyzer :: STRING?, description :: STRING?)",
                        "List the available analyzers that the fulltext indexes can be configured with.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.index.fulltext.queryNodes", "(indexName :: STRING?, queryString :: STRING?) :: (node :: NODE?, score :: FLOAT?)",
                        "Query the given fulltext index. Returns the matching nodes and their lucene query score, ordered by score.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ), proc( "db.index.fulltext.queryRelationships",
                        "(indexName :: STRING?, queryString :: STRING?) :: (relationship :: RELATIONSHIP?, " + "score :: FLOAT?)",
                        "Query the given fulltext index. Returns the matching relationships and their lucene query score, ordered by " + "score.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.prepareForReplanning", "(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Triggers an index resample and waits for it to complete, and after that clears query caches." +
                                " After this procedure has finished queries will be planned using the latest database " + "statistics.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.stats.retrieve", "(section :: STRING?, config = {} :: MAP?) :: (section :: STRING?, data :: MAP?)",
                        "Retrieve statistical data about the current database. Valid sections are 'GRAPH COUNTS', 'TOKENS', 'QUERIES', 'META'",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.stats.retrieveAllAnonymized", "(graphToken :: STRING?, config = {} :: MAP?) :: (section :: STRING?, data :: MAP?)",
                        "Retrieve all available statistical data about the current database, in an anonymized form.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.stats.status", "() :: (section :: STRING?, status :: STRING?, data :: MAP?)",
                        "Retrieve the status of all available collector daemons, for this database.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.stats.collect", "(section :: STRING?, config = {} :: MAP?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                        "Start data collection of a given data section. Valid sections are 'QUERIES'",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.stats.stop", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                        "Stop data collection of a given data section. Valid sections are 'QUERIES'",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "db.stats.clear", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                        "Clear collected data of a given data section. Valid sections are 'QUERIES'",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "READ" ),
                proc( "dbms.routing.getRoutingTable", "(context :: MAP?, database = null :: STRING?) :: (ttl :: INTEGER?, servers :: LIST? OF MAP?)",
                        "Returns endpoints of this instance.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.cluster.routing.getRoutingTable", "(context :: MAP?, database = null :: STRING?) :: (ttl :: INTEGER?, servers :: LIST? OF MAP?)",
                        "Returns endpoints of this instance.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                // enterprise only functions
                proc( "dbms.listTransactions",
                        "() :: (transactionId :: STRING?, username :: STRING?, metaData :: MAP?, startTime :: STRING?, protocol :: STRING?," +
                                " clientAddress :: STRING?, requestUri :: STRING?, currentQueryId :: STRING?, currentQuery :: STRING?, " +
                                "activeLockCount :: INTEGER?, status :: STRING?, resourceInformation :: MAP?, elapsedTimeMillis :: INTEGER?, " +
                                "cpuTimeMillis :: INTEGER?, waitTimeMillis :: INTEGER?, idleTimeMillis :: INTEGER?, allocatedBytes :: INTEGER?, " +
                                "allocatedDirectBytes :: INTEGER?, pageHits :: INTEGER?, pageFaults :: INTEGER?, connectionId :: STRING?, " +
                                "initializationStackTrace :: STRING?)",
                        "List all transactions currently executing at this instance that are visible to the user.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.killQuery", "(id :: STRING?) :: (queryId :: STRING?, username :: STRING?, message :: STRING?)",
                        "Kill all transactions executing the query with the given query id.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.killTransactions", "(ids :: LIST? OF STRING?) :: (transactionId :: STRING?, username :: STRING?, message :: STRING?)",
                        "Kill transactions with provided ids.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.functions",
                        "() :: (name :: STRING?, signature :: STRING?, description :: STRING?, aggregating :: BOOLEAN?, roles :: LIST? OF STRING?)",
                        "List all functions in the DBMS.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.procedures",
                        "() :: (name :: STRING?, signature :: STRING?, description :: STRING?, mode :: STRING?, roles :: LIST? OF STRING?, " +
                                "worksOnSystem :: BOOLEAN?)",
                        "List all procedures in the DBMS.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.setTXMetaData", "(data :: MAP?) :: VOID",
                        "Attaches a map of data to the transaction. The data will be printed when listing queries, and inserted into the query log.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS", false ),
                proc( "dbms.scheduler.profile", "(method :: STRING?, group :: STRING?, duration :: STRING?) :: (profile :: STRING?)",
                        "Begin profiling all threads within the given job group, for the specified duration. Note that profiling incurs" +
                                " overhead to a system, and will slow it down.",
                        stringArray( "admin" ), "DBMS" ), proc( "dbms.getTXMetaData", "() :: (metadata :: MAP?)", "Provides attached transaction metadata.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.killQueries", "(ids :: LIST? OF STRING?) :: (queryId :: STRING?, username :: STRING?, message :: STRING?)",
                        "Kill all transactions executing a query with any of the given query ids.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ), proc( "dbms.listQueries",
                        "() :: (queryId :: STRING?, username :: STRING?, metaData :: MAP?, query :: STRING?, parameters :: MAP?," +
                                " planner :: STRING?, runtime :: STRING?, indexes :: LIST? OF MAP?, startTime :: STRING?, protocol :: STRING?, " +
                                "clientAddress :: STRING?, requestUri :: STRING?, status :: STRING?, resourceInformation :: MAP?, " +
                                "activeLockCount :: INTEGER?, " +
                                "elapsedTimeMillis :: INTEGER?, cpuTimeMillis :: INTEGER?, waitTimeMillis :: INTEGER?, idleTimeMillis :: INTEGER?, " +
                                "allocatedBytes :: INTEGER?, pageHits :: INTEGER?, pageFaults :: INTEGER?, connectionId :: STRING?)",
                        "List all queries currently executing at this instance that are visible to the user.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "db.createNodeKey", "(index :: STRING?, providerName :: STRING?) :: (index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a node key constraint with index backed by specified index provider " +
                                "(for example: CALL db.createNodeKey(\":Person(name)\", \"lucene+native-2.0\")) - YIELD index, providerName, status",
                        stringArray( "architect", "admin" ), "SCHEMA", false ),
                proc( "dbms.listActiveLocks", "(queryId :: STRING?) :: (mode :: STRING?, resourceType :: STRING?, resourceId :: INTEGER?)",
                        "List the active lock requests granted for the transaction executing the query with the given query id.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.setConfigValue", "(setting :: STRING?, value :: STRING?) :: VOID",
                        "Updates a given setting value. Passing an empty value will result in removing the configured value and falling back to the " +
                                "default value. Changes will not persist and will be lost if the server is restarted.",
                        stringArray( "admin" ), "DBMS" ),
                proc( "dbms.killConnection", "(id :: STRING?) :: (connectionId :: STRING?, username :: STRING?, message :: STRING?)",
                        "Kill network connection with the given connection id.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.checkpoint", "() :: (success :: BOOLEAN?, message :: STRING?)",
                        "Initiate and wait for a new check point, or wait any already on-going check point to complete. " +
                                "Note that this temporarily disables the `dbms.checkpoint.iops.limit` setting in order to make the check point " +
                                "complete faster." +
                                " This might cause transaction throughput to degrade slightly, due to increased IO load.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.scheduler.groups", "() :: (group :: STRING?, threads :: INTEGER?)",
                        "List the job groups that are active in the database internal job scheduler.", stringArray( "admin" ), "DBMS" ),
                proc( "dbms.killConnections", "(ids :: LIST? OF STRING?) :: (connectionId :: STRING?, username :: STRING?, message :: STRING?)",
                        "Kill all network connections with the given connection ids.", stringArray( "reader", "editor", "publisher", "architect", "admin" ),
                        "DBMS" ), proc( "dbms.killTransaction", "(id :: STRING?) :: (transactionId :: STRING?, username :: STRING?, message :: STRING?)",
                        "Kill transaction with provided id.", stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ),
                proc( "dbms.listConnections",
                        "() :: (connectionId :: STRING?, connectTime :: STRING?, connector :: STRING?, username :: STRING?, " +
                                "userAgent :: STRING?, serverAddress :: STRING?, clientAddress :: STRING?)",
                        "List all accepted network connections at this instance that are visible to the user.",
                        stringArray( "reader", "editor", "publisher", "architect", "admin" ), "DBMS" ) );
    }

    private AnyValue[] proc( String procName, String procSignature, String description, String mode, boolean worksOnSystem )
    {
        return new AnyValue[]{stringValue( procName ), stringValue( procName + procSignature ), stringValue( description ), stringValue( mode ),
                booleanValue( worksOnSystem )};
    }

    private AnyValue[] proc( String procName, String procSignature, String description, String mode )
    {
        return proc( procName, procSignature, description, mode, true );
    }

    private AnyValue[] proc( String procName, String procSignature, String description, TextArray roles, String mode )
    {
        return proc( procName, procSignature, description, roles, mode, true );
    }

    private AnyValue[] proc( String procName, String procSignature, String description, TextArray roles, String mode, boolean worksOnSystem )
    {
        return new AnyValue[]{stringValue( procName ), stringValue( procName + procSignature ), stringValue( description ), stringValue( mode ), roles,
                booleanValue( worksOnSystem )};
    }

    @SuppressWarnings( {"unchecked", "SameParameterValue"} )
    private Object[] proc( String procName, String procSignature, Matcher<String> description, String mode, boolean worksOnSystem )
    {
        Matcher<AnyValue> desc = new TypeSafeMatcher<>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendText( "invalid description" );
            }

            @Override
            protected boolean matchesSafely( AnyValue item )
            {
                return item instanceof TextValue && description.matches( ((TextValue) item).stringValue() );
            }
        };

        return new Object[]{stringValue( procName ), stringValue( procName + procSignature ), desc, stringValue( mode ), booleanValue( worksOnSystem )};
    }

    @SuppressWarnings( {"unchecked", "SameParameterValue"} )
    private Object[] proc( String procName, String procSignature, Matcher<String> description, TextArray roles, String mode, boolean worksOnSystem )
    {
        Matcher<AnyValue> desc = new TypeSafeMatcher<>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendText( "invalid description" );
            }

            @Override
            protected boolean matchesSafely( AnyValue item )
            {
                return item instanceof TextValue && description.matches( ((TextValue) item).stringValue() );
            }
        };

        return new Object[]{stringValue( procName ), stringValue( procName + procSignature ), desc, stringValue( mode ), roles, booleanValue( worksOnSystem )};
    }
}
