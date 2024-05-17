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
package org.neo4j.consistency;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.consistency.checking.ConsistencyFlags.ALL;
import static org.neo4j.index.internal.gbptree.GBPTreeCorruption.pageSpecificCorruption;
import static org.neo4j.index.internal.gbptree.GBPTreeOpenOptions.NO_FLUSH_ON_CLOSE;
import static org.neo4j.index.internal.gbptree.RootLayerConfiguration.singleRoot;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.mutable.MutableObject;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.index.internal.gbptree.GBPTreeBootstrapper;
import org.neo4j.index.internal.gbptree.GBPTreeCorruption;
import org.neo4j.index.internal.gbptree.GBPTreeInspection;
import org.neo4j.index.internal.gbptree.GBPTreePointerType;
import org.neo4j.index.internal.gbptree.InspectingVisitor;
import org.neo4j.index.internal.gbptree.LayoutBootstrapper;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree;
import org.neo4j.internal.counts.CountsLayout;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.IndexFiles;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.impl.index.schema.SchemaLayouts;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestNeo4jDatabaseManagementServiceBuilder;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsistencyCheckWithCorruptGBPTreeIT {
    private static final Label label = Label.label("label");
    private static final String propKey1 = "key1";

    private static Path neo4jHome;
    // Created in @BeforeAll, contain full dbms with schema index backed by range-1.0 and token indexes
    private EphemeralFileSystemAbstraction sourceSnapshot;
    // Database layout for database created in @BeforeAll
    private RecordDatabaseLayout databaseLayout;
    // Re-instantiated in @BeforeEach using sourceSnapshot
    private EphemeralFileSystemAbstraction fs;
    private ImmutableSet<OpenOption> openOptions;

    private Path labelTokenIndexFile;
    private Path relationshipTypeIndexFile;

    @BeforeAll
    void createIndex() throws Exception {
        final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        TestDirectory testDirectory = TestDirectory.testDirectory(fs);
        testDirectory.prepareDirectory(getClass(), "CorruptGBPTreeIT");
        neo4jHome = testDirectory.homePath();
        fs.mkdirs(neo4jHome);
        var dbms = ((TestNeo4jDatabaseManagementServiceBuilder) new TestDatabaseManagementServiceBuilder(neo4jHome)
                        .setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name())
                        .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fs)))
                .build();
        try {
            final GraphDatabaseService db = dbms.database(DEFAULT_DATABASE_NAME);
            createIndex(db, label);
            forceCheckpoint(db);
            createStringData(db, label);
            databaseLayout = RecordDatabaseLayout.cast(((GraphDatabaseAPI) db).databaseLayout());
            setTokenIndexFiles(db);
            openOptions = ((GraphDatabaseAPI) db)
                    .getDependencyResolver()
                    .resolveDependency(StorageEngine.class)
                    .getOpenOptions();
        } finally {
            dbms.shutdown();
        }
        sourceSnapshot = fs.snapshot();
    }

    @BeforeEach
    void restoreSnapshot() {
        doRestoreSnapshot(sourceSnapshot);
    }

    private void doRestoreSnapshot(EphemeralFileSystemAbstraction snapshot) {
        fs = snapshot.snapshot();
    }

    @Test
    void simpleTestWithNoSetup() throws Exception {
        MutableObject<Integer> heightRef = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> heightRef.setValue(inspection.single().lastLevel()),
                indexFiles);

        final int height = heightRef.getValue();
        assertEquals(
                2,
                height,
                "This test assumes height of index tree is 2 but height for this index was " + height
                        + ". This is most easily regulated by changing number of nodes in setup.");
    }

    @Test
    void assertTreeHeightIsAsExpected() throws Exception {
        MutableObject<Integer> heightRef = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> heightRef.setValue(inspection.single().lastLevel()),
                indexFiles);

        final int height = heightRef.getValue();
        assertEquals(
                2,
                height,
                "This test assumes height of index tree is 2 but height for this index was " + height
                        + ". This is most easily regulated by changing number of nodes in setup.");
    }

    @Test
    void shouldNotCheckIndexesIfConfiguredNotTo() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(targetNode.getValue(), GBPTreeCorruption.notATreeNode()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyFlags flags = ConsistencyFlags.ALL.withoutCheckIndexes();
        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance(), flags);

        assertTrue(result.isSuccessful(), "Expected store to be consistent when not checking indexes.");
    }

    @Test
    void shouldReportProgress() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance(), output);

        assertTrue(result.isSuccessful(), "Expected new database to be clean.");
        assertTrue(output.toString().contains("Index structure consistency check"));
    }

    @Test
    void notATreeNode() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(targetNode.getValue(), GBPTreeCorruption.notATreeNode()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Page: " + targetNode.getValue() + " is not a tree node page.");
    }

    @Test
    void unknownTreeNodeType() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(targetNode.getValue(), GBPTreeCorruption.unknownTreeNodeType()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Page: " + targetNode.getValue() + " has an unknown tree node type:");
    }

    @Test
    void siblingsDontPointToEachOther() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().leafNodes().get(0));
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(), GBPTreeCorruption.rightSiblingPointToNonExisting()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Sibling pointers misaligned.");
    }

    @Test
    void rightmostNodeHasRightSibling() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    final long root = inspection.single().rootNode();
                    tree.unsafe(
                            pageSpecificCorruption(
                                    root, GBPTreeCorruption.setPointer(GBPTreePointerType.rightSibling(), 10)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Expected rightmost node to have no right sibling but was 10");
    }

    @Test
    void pointerToOldVersionOfTreeNode() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(),
                                    GBPTreeCorruption.setPointer(GBPTreePointerType.successor(), 6)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result,
                "We ended up on tree node " + targetNode.getValue() + " which has a newer generation, successor is: 6");
    }

    @Test
    void pointerHasLowerGenerationThanNode() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        MutableObject<Long> rightSibling = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    final ImmutableLongList leafNodes = inspection.single().leafNodes();
                    targetNode.setValue(leafNodes.get(0));
                    rightSibling.setValue(leafNodes.get(1));
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(), GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result,
                String.format(
                        "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d.",
                        GBPTreePointerType.rightSibling(), targetNode.getValue(), 1, rightSibling.getValue(), 7));
    }

    @Test
    void keysOutOfOrderInNode() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().leafNodes().get(0));
                    int keyCount = inspection.single().keyCounts().get(targetNode.getValue());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(), GBPTreeCorruption.swapKeyOrderLeaf(0, 1, keyCount)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result, String.format("Keys in tree node %d are out of order.", targetNode.getValue()));
    }

    @Test
    void keysLocatedInWrongNode() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    final long internalNode =
                            inspection.single().nodesPerLevel().get(1).get(0);
                    int keyCount = inspection.single().keyCounts().get(internalNode);
                    tree.unsafe(
                            pageSpecificCorruption(internalNode, GBPTreeCorruption.swapChildOrder(0, 1, keyCount)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Expected range for this tree node is");
    }

    @Test
    void unusedPage() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    final long internalNode =
                            inspection.single().nodesPerLevel().get(1).get(0);
                    int keyCount = inspection.single().keyCounts().get(internalNode);
                    tree.unsafe(
                            pageSpecificCorruption(internalNode, GBPTreeCorruption.setKeyCount(keyCount - 1)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Index has a leaked page that will never be reclaimed, pageId=");
    }

    @Test
    void pageIdExceedLastId() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) ->
                        tree.unsafe(GBPTreeCorruption.decrementFreelistWritePos(), CursorContext.NULL_CONTEXT),
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Index has a leaked page that will never be reclaimed, pageId=");
    }

    @Test
    void nodeMetaInconsistency() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    tree.unsafe(
                            pageSpecificCorruption(
                                    inspection.single().rootNode(),
                                    GBPTreeCorruption.decrementAllocOffsetInDynamicNode()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "has inconsistent meta data: Meta data for tree node is inconsistent");
    }

    @Test
    void pageIdSeenMultipleTimes() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(GBPTreeCorruption.addFreelistEntry(targetNode.getValue()), CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result,
                "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId="
                        + targetNode.getValue());
    }

    @Test
    void crashPointer() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                false,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(),
                                    GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Crashed pointer found in tree node " + targetNode.getValue());
    }

    @Test
    void brokenPointer() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(), GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Broken pointer found in tree node " + targetNode.getValue());
    }

    @Test
    void unreasonableKeyCount() throws Exception {
        MutableObject<Long> targetNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    targetNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    targetNode.getValue(), GBPTreeCorruption.setKeyCount(Integer.MAX_VALUE)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result, "Unexpected keyCount on pageId " + targetNode.getValue() + ", keyCount=" + Integer.MAX_VALUE);
    }

    @Test
    void childNodeFoundAmongParentNodes() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    final long rootNode = inspection.single().rootNode();
                    tree.unsafe(
                            pageSpecificCorruption(rootNode, GBPTreeCorruption.setChild(0, rootNode)),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(result, "Circular reference, child tree node found among parent nodes. Parents:");
    }

    @Test
    void exception() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    final long rootNode = inspection.single().rootNode();
                    tree.unsafe(
                            pageSpecificCorruption(rootNode, GBPTreeCorruption.setHighestReasonableKeyCount()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result,
                "Caught exception during consistency check: org.neo4j.index.internal.gbptree.TreeInconsistencyException: Some internal problem causing out of"
                        + " bounds: pageId:");
    }

    @Test
    void dirtyOnStartup() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    tree.unsafe(GBPTreeCorruption.makeDirty(), CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertTrue(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result,
                "Index was dirty on startup which means it was not shutdown correctly and need to be cleaned up with a successful recovery.");
    }

    @Test
    void shouldIncludeIndexFileInConsistencyReport() throws Exception {
        Path[] indexFiles = schemaIndexFiles();
        List<Path> corruptedFiles = corruptIndexes(
                true,
                (tree, inspection) -> {
                    final long rootNode = inspection.single().rootNode();
                    tree.unsafe(
                            pageSpecificCorruption(rootNode, GBPTreeCorruption.notATreeNode()),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());

        assertFalse(result.isSuccessful(), "Expected store to be considered inconsistent.");
        assertResultContainsMessage(
                result, "Index file: " + corruptedFiles.get(0).toAbsolutePath());
    }

    @Test
    void multipleCorruptions() throws Exception {
        MutableObject<Long> internalNode = new MutableObject<>();
        Path[] indexFiles = schemaIndexFiles();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    long leafNode = inspection.single().leafNodes().get(0);
                    internalNode.setValue(
                            inspection.single().nodesPerLevel().get(1).get(0));
                    final Integer internalNodeKeyCount =
                            inspection.single().keyCounts().get(internalNode.getValue());
                    tree.unsafe(
                            pageSpecificCorruption(leafNode, GBPTreeCorruption.rightSiblingPointToNonExisting()),
                            CursorContext.NULL_CONTEXT);
                    tree.unsafe(
                            pageSpecificCorruption(
                                    internalNode.getValue(),
                                    GBPTreeCorruption.swapChildOrder(0, 1, internalNodeKeyCount)),
                            CursorContext.NULL_CONTEXT);
                    tree.unsafe(
                            pageSpecificCorruption(
                                    internalNode.getValue(),
                                    GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                indexFiles);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());
        assertResultContainsMessage(result, "Index inconsistency: Sibling pointers misaligned.");
        assertResultContainsMessage(result, "Index inconsistency: Expected range for this tree node is");
        assertResultContainsMessage(
                result,
                "Index inconsistency: Broken pointer found in tree node " + internalNode.getValue()
                        + ", pointerType='left sibling'");
        assertResultContainsMessage(result, "Index inconsistency: Pointer (left sibling) in tree node ");
    }

    @Test
    void corruptionInNodeLabelIndex() throws Exception {
        MutableObject<Long> rootNode = new MutableObject<>();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    rootNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    rootNode.getValue(), GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                labelTokenIndexFile);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());
        assertFalse(result.isSuccessful());
        assertResultContainsMessage(
                result,
                "Index inconsistency: Broken pointer found in tree node " + rootNode.getValue()
                        + ", pointerType='left sibling'");
        assertResultContainsMessage(result, "Number of inconsistent LABEL_SCAN_DOCUMENT records: 1");
    }

    @Test
    void corruptionInRelationshipTypeIndex() throws Exception {
        MutableObject<Long> rootNode = new MutableObject<>();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    rootNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    rootNode.getValue(), GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                relationshipTypeIndexFile);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());
        assertFalse(result.isSuccessful());
        assertResultContainsMessage(
                result,
                "Index inconsistency: Broken pointer found in tree node " + rootNode.getValue()
                        + ", pointerType='left sibling'");
        assertResultContainsMessage(result, "Number of inconsistent RELATIONSHIP_TYPE_SCAN_DOCUMENT records: 1");
    }

    @Test
    void corruptionInIndexStatisticsStore() throws Exception {
        MutableObject<Long> rootNode = new MutableObject<>();
        Path indexStatisticsStoreFile = indexStatisticsStoreFile();
        corruptIndexes(
                true,
                (tree, inspection) -> {
                    rootNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    rootNode.getValue(), GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                indexStatisticsStoreFile);

        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance());
        assertFalse(result.isSuccessful());
        assertResultContainsMessage(
                result,
                "Index inconsistency: Broken pointer found in tree node " + rootNode.getValue()
                        + ", pointerType='left sibling'");
        assertResultContainsMessage(result, "Number of inconsistent INDEX_STATISTICS records: 1");
    }

    @Test
    void corruptionInCountsStore() throws Exception {
        MutableObject<Long> rootNode = new MutableObject<>();
        Path countsStoreFile = countsStoreFile();
        final LayoutBootstrapper countsLayoutBootstrapper =
                meta -> new LayoutBootstrapper.Layouts(new CountsLayout(), singleRoot());
        corruptIndexes(
                fs,
                true,
                (tree, inspection) -> {
                    rootNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    rootNode.getValue(), GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                countsLayoutBootstrapper,
                countsStoreFile);

        ConsistencyFlags flags = ConsistencyFlags.NONE.withCheckCounts().withCheckStructure();
        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance(), flags);
        assertFalse(result.isSuccessful());
        assertResultContainsMessage(
                result,
                "Index inconsistency: Broken pointer found in tree node " + rootNode.getValue()
                        + ", pointerType='left sibling'");
        assertResultContainsMessage(result, "Number of inconsistent COUNTS records: 1");
    }

    @Test
    void corruptionInIdGenerator() throws Exception {
        MutableObject<Long> rootNode = new MutableObject<>();
        Path[] idStoreFiles = idStoreFiles();
        corruptIndexes(
                fs,
                true,
                (tree, inspection) -> {
                    rootNode.setValue(inspection.single().rootNode());
                    tree.unsafe(
                            pageSpecificCorruption(
                                    rootNode.getValue(), GBPTreeCorruption.broken(GBPTreePointerType.leftSibling())),
                            CursorContext.NULL_CONTEXT);
                },
                idStoreFiles);

        ConsistencyFlags flags = ConsistencyFlags.NONE.withCheckStructure();
        ConsistencyCheckService.Result result = runConsistencyCheck(NullLogProvider.getInstance(), flags);
        assertFalse(result.isSuccessful());
        assertResultContainsMessage(
                result,
                "Index inconsistency: Broken pointer found in tree node " + rootNode.getValue()
                        + ", pointerType='left sibling'");
        assertResultContainsMessage(result, "Number of inconsistent ID_STORE records: " + idStoreFiles.length);
    }

    private void assertResultContainsMessage(ConsistencyCheckService.Result result, String expectedMessage)
            throws IOException {
        assertResultContainsMessage(new DefaultFileSystemAbstraction(), result, expectedMessage);
    }

    private static void assertResultContainsMessage(
            FileSystemAbstraction fs, ConsistencyCheckService.Result result, String expectedMessage)
            throws IOException {
        List<String> lines = FileSystemUtils.readLines(fs, result.reportFile(), EmptyMemoryTracker.INSTANCE);
        boolean reportContainExpectedMessage = false;
        for (String line : lines) {
            if (line.contains(expectedMessage)) {
                reportContainExpectedMessage = true;
                break;
            }
        }
        String errorMessage = format(
                "Expected consistency report to contain message `%s'. Real result was: %s%n",
                expectedMessage, String.join(System.lineSeparator(), lines));
        assertTrue(reportContainExpectedMessage, errorMessage);
    }

    private ConsistencyCheckService.Result runConsistencyCheck(InternalLogProvider logProvider)
            throws ConsistencyCheckIncompleteException {
        return runConsistencyCheck(logProvider, (OutputStream) null);
    }

    private ConsistencyCheckService.Result runConsistencyCheck(
            InternalLogProvider logProvider, ConsistencyFlags consistencyFlags)
            throws ConsistencyCheckIncompleteException {
        return runConsistencyCheck(logProvider, null, consistencyFlags);
    }

    private ConsistencyCheckService.Result runConsistencyCheck(
            InternalLogProvider logProvider, OutputStream progressOutput) throws ConsistencyCheckIncompleteException {
        return runConsistencyCheck(logProvider, progressOutput, ALL);
    }

    private ConsistencyCheckService.Result runConsistencyCheck(
            InternalLogProvider logProvider, OutputStream progressOutput, ConsistencyFlags consistencyFlags)
            throws ConsistencyCheckIncompleteException {
        Config config = Config.newBuilder().set(neo4j_home, neo4jHome).build();
        return new ConsistencyCheckService(databaseLayout)
                .with(config)
                .with(progressOutput)
                .with(logProvider)
                .with(fs)
                .with(consistencyFlags)
                .runFullConsistencyCheck();
    }

    private Path indexStatisticsStoreFile() {
        return databaseLayout.indexStatisticsStore();
    }

    private Path countsStoreFile() {
        return databaseLayout.countStore();
    }

    private Path[] idStoreFiles() {
        return databaseLayout.idFiles().toArray(new Path[0]);
    }

    private Path[] schemaIndexFiles() throws IOException {
        final Path databaseDir = databaseLayout.databaseDirectory();
        return schemaIndexFiles(fs, databaseDir, RangeIndexProvider.DESCRIPTOR);
    }

    private static Path[] schemaIndexFiles(
            FileSystemAbstraction fs, Path databaseDir, IndexProviderDescriptor indexProvider) throws IOException {
        final String fileNameFriendlyProviderName = IndexDirectoryStructure.fileNameFriendly(indexProvider.name());
        Path indexDir = databaseDir.resolve("schema/index/");
        return fs.streamFilesRecursive(indexDir)
                .map(FileHandle::getPath)
                .filter(path -> path.toAbsolutePath().toString().contains(fileNameFriendlyProviderName))
                .toArray(Path[]::new);
    }

    private List<Path> corruptIndexes(boolean readOnly, CorruptionInject corruptionInject, Path... targetFiles)
            throws Exception {
        return corruptIndexes(fs, readOnly, corruptionInject, targetFiles);
    }

    private List<Path> corruptIndexes(
            FileSystemAbstraction fs, boolean readOnly, CorruptionInject corruptionInject, Path... targetFiles)
            throws Exception {
        return corruptIndexes(fs, readOnly, corruptionInject, new SchemaLayouts(), targetFiles);
    }

    private List<Path> corruptIndexes(
            FileSystemAbstraction fs,
            boolean readOnly,
            CorruptionInject corruptionInject,
            LayoutBootstrapper layoutBootstrapper,
            Path... targetFiles)
            throws Exception {
        List<Path> treeFiles = new ArrayList<>();
        var contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);
        try (var cursorContext = contextFactory.create("corruptIndexes");
                JobScheduler jobScheduler = createInitialisedScheduler();
                GBPTreeBootstrapper bootstrapper =
                        new GBPTreeBootstrapper(fs, jobScheduler, layoutBootstrapper, readOnly, contextFactory, NULL)) {
            for (Path file : targetFiles) {
                var bootstrap = bootstrapper.bootstrapTree(
                        file, openOptions.newWith(NO_FLUSH_ON_CLOSE).toArray(new OpenOption[0]));
                if (bootstrap.isTree()) {
                    treeFiles.add(file);
                    try (MultiRootGBPTree<?, ?, ?> gbpTree = bootstrap.tree()) {
                        InspectingVisitor<?, ?, ?> visitor = gbpTree.visit(new InspectingVisitor<>(), cursorContext);
                        corruptionInject.corrupt(gbpTree, visitor.get());
                    }
                }
            }
        }
        assertThat(treeFiles)
                .withFailMessage("No index files corrupted, check that bootstrap of the files work correctly")
                .isNotEmpty();
        return treeFiles;
    }

    private static void createStringData(GraphDatabaseService db, Label label) {
        String longString = longString();

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 60; i++) {
                Node node = tx.createNode(label);
                // Using long string that only differ in the end make sure index tree will be higher which we need to
                // mess up internal pointers
                String value = longString + i;
                node.setProperty(propKey1, value);
            }
            tx.commit();
        }
    }

    private static void createIndex(GraphDatabaseService db, Label label) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label).on(propKey1).create();
            tx.commit();
        }
        awaitIndexes(db);
    }

    private static void forceCheckpoint(GraphDatabaseService db) throws IOException {
        var checkPointer = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.tryCheckPoint(new SimpleTriggerInfo("Initial checkpoint"));
    }

    private static void awaitIndexes(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
    }

    private static String longString() {
        char[] chars = new char[1000];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }

    private void setTokenIndexFiles(GraphDatabaseService db) {
        try (var tx = db.beginTx()) {
            StreamSupport.stream(tx.schema().getIndexes().spliterator(), false)
                    .filter(idx -> idx.getIndexType() == IndexType.LOOKUP)
                    .forEach(idx -> {
                        IndexDirectoryStructure indexDirectoryStructure = IndexDirectoryStructure.directoriesByProvider(
                                        databaseLayout.databaseDirectory())
                                .forProvider(TokenIndexProvider.DESCRIPTOR);
                        long id =
                                ((IndexDefinitionImpl) idx).getIndexReference().getId();
                        IndexFiles indexFiles = new IndexFiles(fs, indexDirectoryStructure, id);
                        if (idx.isNodeIndex()) {
                            labelTokenIndexFile = indexFiles.getStoreFile();
                        } else {
                            relationshipTypeIndexFile = indexFiles.getStoreFile();
                        }
                    });
        }
    }

    @FunctionalInterface
    private interface CorruptionInject {
        void corrupt(MultiRootGBPTree<?, ?, ?> tree, GBPTreeInspection inspection) throws IOException;
    }
}
