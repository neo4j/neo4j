/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.integrationtest;

import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Condition;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;

public interface ProcedureITBase {
    default List<Object[]>
            getExpectedCommunityProcs() // they have a column for roles but that should be ignored on community and
                // expected to be null
            {
        return List.of(
                proc(
                        "db.info",
                        "() :: (id :: STRING, name :: STRING, creationDate :: STRING)",
                        "Provides information regarding the database.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ",
                        true),
                proc(
                        "dbms.info",
                        "() :: (id :: STRING, name :: STRING, creationDate :: STRING)",
                        "Provides information regarding the DBMS.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS",
                        true),
                proc(
                        "dbms.listConfig",
                        "(searchString =  :: STRING) :: (name :: STRING, description :: STRING, value :: STRING, dynamic :: BOOLEAN)",
                        "List the currently active configuration settings of Neo4j.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "db.awaitIndex",
                        "(indexName :: STRING, timeOutSeconds = 300 :: INTEGER)",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\"MyIndex\", 300)).",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.awaitIndexes",
                        "(timeOutSeconds = 300 :: INTEGER)",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(300)).",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.resampleIndex",
                        "(indexName :: STRING)",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\"MyIndex\")).",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.resampleOutdatedIndexes",
                        "()",
                        "Schedule resampling of all outdated indexes.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.propertyKeys",
                        "() :: (propertyKey :: STRING)",
                        "List all property keys in the database.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.labels",
                        "() :: (label :: STRING)",
                        "List all available labels in the database.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.schema.visualization",
                        "() :: (nodes :: LIST<NODE>, relationships :: LIST<RELATIONSHIP>)",
                        "Visualizes the schema of the data based on available statistics. A new node is "
                                + "returned for each label. The properties represented on the node include: `name` "
                                + "(label name), `indexes` (list of indexes), and `constraints` (list of constraints). "
                                + "A relationship of a given type is returned for all possible combinations of start and end nodes. "
                                + "The properties represented on the relationship include: `name` (type name). "
                                + "Note that this may include additional relationships that do not exist in the data due to "
                                + "the information available in the count store. ",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.schema.nodeTypeProperties",
                        "() :: (nodeType :: STRING, nodeLabels :: LIST<STRING>, propertyName :: STRING, "
                                + "propertyTypes :: LIST<STRING>, mandatory :: BOOLEAN)",
                        "Show the derived property schema of the nodes in tabular form.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.schema.relTypeProperties",
                        "() :: (relType :: STRING, "
                                + "propertyName :: STRING, propertyTypes :: LIST<STRING>, mandatory :: BOOLEAN)",
                        "Show the derived property schema of the relationships in tabular form.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.relationshipTypes",
                        "() :: (relationshipType :: STRING)",
                        "List all available relationship types in the database.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "dbms.components",
                        "() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)",
                        "List DBMS components and their versions.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.queryJmx",
                        "(query :: STRING) :: (name :: STRING, description :: STRING, attributes :: MAP)",
                        "Query JMX management data by domain and name. For instance, use `*:*` to find all JMX beans.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "db.createLabel",
                        "(newLabel :: STRING)",
                        "Create a label",
                        stringArray("publisher", "architect", "admin"),
                        "WRITE",
                        false),
                proc(
                        "db.createProperty",
                        "(newProperty :: STRING)",
                        "Create a Property",
                        stringArray("publisher", "architect", "admin"),
                        "WRITE",
                        false),
                proc(
                        "db.createRelationshipType",
                        "(newRelationshipType :: STRING)",
                        "Create a RelationshipType",
                        stringArray("publisher", "architect", "admin"),
                        "WRITE",
                        false),
                proc(
                        "db.clearQueryCaches",
                        "() :: (value :: STRING)",
                        "Clears all query caches.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "db.index.fulltext.awaitEventuallyConsistentIndexRefresh",
                        "()",
                        "Wait for the updates from recently committed transactions to be applied to any eventually-consistent full-text indexes.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.index.fulltext.listAvailableAnalyzers",
                        "() :: (analyzer :: STRING, description :: STRING, stopwords :: LIST<STRING>)",
                        "List the available analyzers that the full-text indexes can be configured with.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.index.fulltext.queryNodes",
                        "(indexName :: STRING, queryString :: STRING, options = {} :: MAP) :: (node :: NODE, score :: FLOAT)",
                        "Query the given full-text index. Returns the matching nodes, and their Lucene query score, ordered by score. "
                                + "Valid keys for the options map are: 'skip' to skip the top N results; 'limit' to limit the number of results returned.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.index.fulltext.queryRelationships",
                        "(indexName :: STRING, queryString :: STRING, options = {} :: MAP) :: (relationship :: RELATIONSHIP, "
                                + "score :: FLOAT)",
                        "Query the given full-text index. Returns the matching relationships, and their Lucene query score, ordered by score. "
                                + "Valid keys for the options map are: 'skip' to skip the top N results; 'limit' to limit the number of results returned.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "db.prepareForReplanning",
                        "(timeOutSeconds = 300 :: INTEGER)",
                        "Triggers an index resample and waits for it to complete, and after that clears query caches."
                                + " After this procedure has finished queries will be planned using the latest database "
                                + "statistics.",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "db.stats.retrieve",
                        "(section :: STRING, config = {} :: MAP) :: (section :: STRING, data :: MAP)",
                        "Retrieve statistical data about the current database. Valid sections are 'GRAPH COUNTS', 'TOKENS', 'QUERIES', 'META'",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "db.stats.retrieveAllAnonymized",
                        "(graphToken :: STRING, config = {} :: MAP) :: (section :: STRING, data :: MAP)",
                        "Retrieve all available statistical data about the current database, in an anonymized form.",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "db.stats.status",
                        "() :: (section :: STRING, status :: STRING, data :: MAP)",
                        "Retrieve the status of all available collector daemons, for this database.",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "db.stats.collect",
                        "(section :: STRING, config = {} :: MAP) :: (section :: STRING, success :: BOOLEAN, message :: STRING)",
                        "Start data collection of a given data section. Valid sections are 'QUERIES'",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "db.stats.stop",
                        "(section :: STRING) :: (section :: STRING, success :: BOOLEAN, message :: STRING)",
                        "Stop data collection of a given data section. Valid sections are 'QUERIES'",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "db.stats.clear",
                        "(section :: STRING) :: (section :: STRING, success :: BOOLEAN, message :: STRING)",
                        "Clear collected data of a given data section. Valid sections are 'QUERIES'",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "dbms.routing.getRoutingTable",
                        "(context :: MAP, database = null :: STRING) :: (ttl :: INTEGER, servers :: LIST<MAP>)",
                        "Returns the advertised bolt capable endpoints for a given database, "
                                + "divided by each endpoint's capabilities. "
                                + "For example, an endpoint may serve read queries, write queries, and/or future `getRoutingTable` requests. ",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.cluster.routing.getRoutingTable",
                        "(context :: MAP, database = null :: STRING) :: (ttl :: INTEGER, servers :: LIST<MAP>)",
                        "Returns the advertised bolt capable endpoints for a given database, "
                                + "divided by each endpoint's capabilities. "
                                + "For example, an endpoint may serve read queries, write queries, and/or future `getRoutingTable` requests. ",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "tx.setMetaData",
                        "(data :: MAP)",
                        "Attaches a map of data to the transaction. The data will be printed when listing queries, and inserted into the query log.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS",
                        false),
                proc(
                        "tx.getMetaData",
                        "() :: (metadata :: MAP)",
                        "Provides attached transaction metadata.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "db.ping",
                        "() :: (success :: BOOLEAN)",
                        "This procedure can be used by client side tooling to test whether they are correctly connected to a database. "
                                + "The procedure is available in all databases and always returns true. A faulty connection can be detected by not being able to call "
                                + "this procedure.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ",
                        true),
                proc(
                        "dbms.upgradeStatus",
                        "() :: (status :: STRING, description :: STRING, resolution :: STRING)",
                        "Report the current status of the system database sub-graph schema.",
                        stringArray("admin"),
                        "READ"),
                proc(
                        "dbms.upgrade",
                        "() :: (status :: STRING, upgradeResult :: STRING)",
                        "Upgrade the system database schema if it is not the current schema.",
                        stringArray("admin"),
                        "WRITE"),
                proc(
                        "dbms.killConnection",
                        "(id :: STRING) :: (connectionId :: STRING, username :: STRING, message :: STRING)",
                        "Kill network connection with the given connection id.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.killConnections",
                        "(ids :: LIST<STRING>) :: (connectionId :: STRING, username :: STRING, message :: STRING)",
                        "Kill all network connections with the given connection ids.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.listConnections",
                        "() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, "
                                + "userAgent :: STRING, serverAddress :: STRING, clientAddress :: STRING)",
                        "List all accepted network connections at this instance that are visible to the user.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.listCapabilities",
                        "() :: (name :: STRING, description :: STRING, value :: ANY)",
                        "List capabilities.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"));
    }

    default List<Object[]> getExpectedEnterpriseProcs() {
        List<Object[]> result = new ArrayList<>(getExpectedCommunityProcs());
        result.addAll(List.of(
                // enterprise only functions
                proc(
                        "db.listLocks",
                        "() :: (mode :: STRING, resourceType :: STRING, resourceId :: INTEGER, transactionId :: STRING)",
                        "List all locks at this database.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "dbms.listPools",
                        "() :: (pool :: STRING, databaseName :: STRING, heapMemoryUsed :: STRING, heapMemoryUsedBytes :: STRING, "
                                + "nativeMemoryUsed :: STRING, nativeMemoryUsedBytes :: STRING, freeMemory :: STRING, freeMemoryBytes :: STRING, "
                                + "totalPoolMemory :: STRING, totalPoolMemoryBytes :: STRING)",
                        "List all memory pools, including sub pools, currently registered at this instance that are visible to the user.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.listActiveLocks",
                        "(queryId :: STRING) :: (mode :: STRING, resourceType :: STRING, resourceId :: INTEGER)",
                        "List the active lock requests granted for the transaction executing the query with the given query id.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.setConfigValue",
                        "(setting :: STRING, value :: STRING)",
                        "Update a given setting value. Passing an empty value results in removing the configured value and falling back to the "
                                + "default value. Changes do not persist and are lost if the server is restarted. "
                                + "In a clustered environment, `dbms.setConfigValue` affects only the cluster member it is run against.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "db.checkpoint",
                        "() :: (success :: BOOLEAN, message :: STRING)",
                        "Initiate and wait for a new check point, or wait any already on-going check point to complete. "
                                + "Note that this temporarily disables the `db.checkpoint.iops.limit` setting in order to make the check point "
                                + "complete faster."
                                + " This might cause transaction throughput to degrade slightly, due to increased IO load.",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "DBMS"),
                proc(
                        "dbms.scheduler.groups",
                        "() :: (group :: STRING, threads :: INTEGER)",
                        "List the job groups that are active in the database internal job scheduler.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "dbms.scheduler.failedJobs",
                        "() :: " + "(jobId :: STRING, group :: STRING, database :: STRING, submitter :: STRING, "
                                + "description :: STRING, type :: STRING, submitted :: STRING, executionStart :: STRING, failureTime :: "
                                + "STRING, failureDescription :: STRING)",
                        "List failed job runs. There is a limit for amount of historical data.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "dbms.scheduler.jobs",
                        "() :: (jobId :: STRING, group :: STRING, submitted :: STRING, database :: STRING, "
                                + "submitter :: STRING, description :: STRING, type :: STRING, scheduledAt :: STRING, period :: STRING, "
                                + "state :: STRING, currentStateDescription :: STRING)",
                        "List all jobs that are active in the database internal job scheduler.",
                        stringArray("admin"),
                        "DBMS"),
                proc(
                        "dbms.cluster.protocols",
                        "() :: (orientation :: STRING, remoteAddress :: STRING, applicationProtocol :: STRING, "
                                + "applicationProtocolVersion :: INTEGER, modifierProtocols :: STRING)",
                        "Overview of installed protocols",
                        stringArray("reader", "editor", "publisher", "architect", "admin"),
                        "READ"),
                proc(
                        "dbms.quarantineDatabase",
                        "(databaseName :: STRING, setStatus :: BOOLEAN, reason = No reason given :: STRING) :: "
                                + "(databaseName :: STRING, quarantined :: BOOLEAN, result :: STRING)",
                        "Place a database into quarantine or remove it from it.",
                        stringArray("admin"),
                        "DBMS")));
        return result;
    }

    private static AnyValue[] proc(
            String procName, String procSignature, String description, TextArray roles, String mode) {
        return proc(procName, procSignature, description, roles, mode, true);
    }

    private static AnyValue[] proc(
            String procName,
            String procSignature,
            String description,
            TextArray roles,
            String mode,
            boolean worksOnSystem) {
        return new AnyValue[] {
            stringValue(procName),
            stringValue(procName + procSignature),
            stringValue(description),
            stringValue(mode),
            roles,
            booleanValue(worksOnSystem)
        };
    }

    @SuppressWarnings({"SameParameterValue"})
    private static Object[] proc(
            String procName,
            String procSignature,
            Condition<String> description,
            TextArray roles,
            String mode,
            boolean worksOnSystem) {

        Condition<AnyValue> desc = new Condition<>(
                item -> item instanceof TextValue value && description.matches(value.stringValue()),
                String.format("%s: invalid description", description.description()));

        return new Object[] {
            stringValue(procName),
            stringValue(procName + procSignature),
            desc,
            stringValue(mode),
            roles,
            booleanValue(worksOnSystem)
        };
    }
}
