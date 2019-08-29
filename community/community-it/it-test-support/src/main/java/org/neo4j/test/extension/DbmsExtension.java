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
package org.neo4j.test.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

/**
 * Provides test access to {@link DatabaseManagementService}. If you want an impermanent version, see
 * {@link ImpermanentDbmsExtension}.
 *
 * <p>This extension will inject the following fields, if available.
 * <ul>
 *     <li>{@link FileSystemAbstraction} as {@link DefaultFileSystemExtension}.</li>
 *     <li>{@link TestDirectory}.</li>
 *     <li>{@link DatabaseManagementService}.</li>
 *     <li>{@link GraphDatabaseService}, as specified by {@link #injectableDatabase()}.</li>
 *     <li>{@link GraphDatabaseAPI}, as specified by {@link #injectableDatabase()}.</li>
 * </ul>
 *
 * <p>You can specify a callback with {@link #configurationCallback()}, this callback is invoked just before
 * the {@link DatabaseManagementServiceBuilder} completes, allowing further modifications, e.g. adding additional
 * dependencies, injecting a monitor or extension etc.
 *
 * <p>The annotation can be added to the entire test class or per test method. If any configuration values is
 * added to the annotation, the one closest to the target will be chosen. E.g. if you add the annotation to the
 * test class with a configuration, you could then add the annotation to a specific test method and thus override
 * the configuration for that method. The other test methods will not be affected by this.
 */
@Inherited
@Target( {ElementType.TYPE, ElementType.METHOD} )
@Retention( RetentionPolicy.RUNTIME )
@TestDirectoryExtension
@ExtendWith( DbmsSupportExtension.class )
public @interface DbmsExtension
{
    /**
     * The name of the database to inject into the {@link GraphDatabaseService} and {@link GraphDatabaseAPI} fields.
     * A typical use case is to specify {@link GraphDatabaseSettings#SYSTEM_DATABASE_NAME} to execute your test
     * against the system database.
     */
    String injectableDatabase() default GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    /**
     * Name of a void method that takes a {@link TestDatabaseManagementServiceBuilder} as parameter. The method
     * must be annotated with {@link ExtensionCallback}. This can be used to issue additional commands on the
     * builder before it completes. This method will be invoked <strong>BEFORE</strong> any method annotated with
     * {@link BeforeEach}, this is because the injected fields should be available in the BeforeEach context.
     * Setting it to {@code null} or an empty string will disable it.
     *
     * <p>One example is to set some additional configuration values:
     * <pre>{@code
     *     @ExtensionCallback
     *     void configuration( TestDatabaseManagementServiceBuilder builder )
     *     {
     *         builder.setConfig( ... );
     *     }
     * }</pre>
     */
    String configurationCallback() default "";
}
