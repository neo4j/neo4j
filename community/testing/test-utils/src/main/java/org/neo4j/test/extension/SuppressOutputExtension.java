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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.commons.util.AnnotationUtils;

public class SuppressOutputExtension extends StatefulFieldExtension<SuppressOutput>
        implements BeforeEachCallback, AfterEachCallback {
    static final String SUPPRESS_OUTPUT = "suppressOutput";
    static final Namespace SUPPRESS_OUTPUT_NAMESPACE = Namespace.create(SUPPRESS_OUTPUT);

    @Override
    protected String getFieldKey() {
        return SUPPRESS_OUTPUT;
    }

    @Override
    protected Class<SuppressOutput> getFieldType() {
        return SuppressOutput.class;
    }

    @Override
    protected SuppressOutput createField(ExtensionContext extensionContext) {
        return SuppressOutput.suppressAll();
    }

    @Override
    protected Namespace getNameSpace() {
        return SUPPRESS_OUTPUT_NAMESPACE;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        getStoredValue(context).releaseVoices(context.getExecutionException().isPresent());
        removeStoredValue(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        assertHasResourceLock(context);
        getStoredValue(context).captureVoices();
    }

    private void assertHasResourceLock(ExtensionContext context) {
        List<ResourceLock> resourceLocks = new ArrayList<>();
        context.getTestMethod()
                .ifPresent(method ->
                        resourceLocks.addAll(AnnotationUtils.findRepeatableAnnotations(method, ResourceLock.class)));
        context.getTestClass().ifPresent(testClass -> {
            Class<?> cls;
            Class<?> host = testClass;
            do {
                cls = host;
                resourceLocks.addAll(AnnotationUtils.findRepeatableAnnotations(cls, ResourceLock.class));
                host = cls.getEnclosingClass();
            } while (host != null);
        });
        if (resourceLocks.stream().noneMatch(resourceLock -> Resources.SYSTEM_OUT.equals(resourceLock.value()))) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " requires `@ResourceLock(Resources.SYSTEM_OUT)` annotation.");
        }
    }
}
