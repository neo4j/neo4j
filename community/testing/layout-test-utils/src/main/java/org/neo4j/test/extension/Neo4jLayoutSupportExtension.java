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
package org.neo4j.test.extension;

import static java.util.Arrays.stream;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstances;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * Be aware that all layouts regarding directory layout provided by this extension,
 * i.e Neo4JLayout, DatabaseLayout is reflecting a default setup.
 * The configuration passed to the DatabaseManagementService is the source of truth,
 * and any custom configuration may cause a mismatch.
 */
public class Neo4jLayoutSupportExtension implements BeforeAllCallback, BeforeEachCallback {
    @Override
    public void beforeAll(ExtensionContext context) {
        if (getLifecycle(context) == PER_CLASS) {
            prepare(context);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        if (getLifecycle(context) == PER_METHOD) {
            prepare(context);
        }
    }

    private void prepare(ExtensionContext context) {
        TestInstances testInstances = context.getRequiredTestInstances();
        TestDirectory testDir = getTestDirectory(context);

        Config config = Config.defaults(neo4j_home, testDir.homePath());
        Neo4jLayout neo4jLayout = Neo4jLayout.of(config);
        DatabaseLayout databaseLayout = RecordDatabaseLayout.of(
                neo4jLayout, config.get(initial_default_database)); // Record format is still default

        createDirectories(testDir.getFileSystem(), neo4jLayout, databaseLayout);

        for (Object testInstance : testInstances.getAllInstances()) {
            injectInstance(testInstance, neo4jLayout);
            injectInstance(testInstance, databaseLayout);
        }
    }

    private static void createDirectories(
            FileSystemAbstraction fs, Neo4jLayout neo4jLayout, DatabaseLayout databaseLayout) {
        createDirectory(fs, neo4jLayout.homeDirectory());
        createDirectory(fs, neo4jLayout.databasesDirectory());
        createDirectory(fs, neo4jLayout.transactionLogsRootDirectory());
        createDirectory(fs, databaseLayout.databaseDirectory());
        createDirectory(fs, databaseLayout.getTransactionLogsDirectory());
    }

    private static void createDirectory(FileSystemAbstraction fs, Path directory) {
        try {
            fs.mkdirs(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create directory: " + directory, e);
        }
    }

    private TestDirectory getTestDirectory(ExtensionContext context) {
        TestDirectory testDir = context.getStore(TestDirectorySupportExtension.TEST_DIRECTORY_NAMESPACE)
                .get(TestDirectorySupportExtension.TEST_DIRECTORY, TestDirectory.class);
        if (testDir == null) {
            throw new IllegalStateException(TestDirectorySupportExtension.class.getSimpleName()
                    + " not in scope, make sure to add it before the "
                    + getClass().getSimpleName());
        }
        return testDir;
    }

    private static <T> void injectInstance(Object testInstance, T instance) {
        Class<?> testClass = testInstance.getClass();
        do {
            stream(testClass.getDeclaredFields())
                    .filter(field -> isAnnotated(field, Inject.class))
                    .filter(field -> field.getType().isAssignableFrom(instance.getClass()))
                    .forEach(field -> setField(testInstance, field, instance));
            testClass = testClass.getSuperclass();
        } while (testClass != null);
    }

    private static void setField(Object testInstance, Field field, Object db) {
        field.setAccessible(true);
        try {
            field.set(testInstance, db);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static TestInstance.Lifecycle getLifecycle(ExtensionContext context) {
        return context.getTestInstanceLifecycle().orElse(PER_METHOD);
    }
}
