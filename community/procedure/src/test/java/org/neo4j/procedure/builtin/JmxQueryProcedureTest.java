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
package org.neo4j.procedure.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.values.storable.Values.stringValue;

import java.lang.management.ManagementFactory;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;

class JmxQueryProcedureTest {
    private MBeanServer jmxServer;
    private ObjectName beanName;
    private String attributeName;

    @BeforeEach
    void setup() throws Throwable {
        jmxServer = mock(MBeanServer.class);
        beanName = new ObjectName("org.neo4j:chevyMakesTheTruck=bobMcCoshMakesTheDifference");
        attributeName = "name";

        when(jmxServer.queryNames(new ObjectName("*:*"), null)).thenReturn(asSet(beanName));
        when(jmxServer.getMBeanInfo(beanName))
                .thenReturn(new MBeanInfo(
                        "org.neo4j.SomeMBean",
                        "This is a description",
                        new MBeanAttributeInfo[] {
                            new MBeanAttributeInfo(
                                    attributeName, "someType", "This is the attribute desc.", true, false, false)
                        },
                        null,
                        null,
                        null));
    }

    @Test
    void shouldHandleBasicMBean() throws Throwable {
        // given
        when(jmxServer.getAttribute(beanName, "name")).thenReturn("Hello, world!");
        JmxQueryProcedure procedure = new JmxQueryProcedure(new QualifiedName("bob"), jmxServer);

        // when
        RawIterator<AnyValue[], ProcedureException> result =
                procedure.apply(null, new AnyValue[] {stringValue("*:*")}, EMPTY_RESOURCE_TRACKER);

        // then
        assertThat(asList(result)).contains(new AnyValue[] {
            stringValue("org.neo4j:chevyMakesTheTruck=bobMcCoshMakesTheDifference"),
            stringValue("This is a description"),
            ValueUtils.of(map(
                    attributeName,
                    map(
                            "description", "This is the attribute desc.",
                            "value", "Hello, world!")))
        });
    }

    @Test
    void shouldHandleMBeanThatThrowsOnGetAttribute() throws Throwable {
        // given some JVM MBeans do not allow accessing their attributes, despite marking
        // then as readable
        when(jmxServer.getAttribute(beanName, "name"))
                // We throw the exact combo thrown by JVM MBeans here, so that any other exception will bubble up,
                // and we can make an informed decision about swallowing more exception on an as-needed basis.
                .thenThrow(new RuntimeMBeanException(
                        new UnsupportedOperationException("Haha, screw discoverable services!")));

        JmxQueryProcedure procedure = new JmxQueryProcedure(new QualifiedName("bob"), jmxServer);

        // when
        RawIterator<AnyValue[], ProcedureException> result =
                procedure.apply(null, new AnyValue[] {stringValue("*:*")}, EMPTY_RESOURCE_TRACKER);

        // then
        assertThat(asList(result)).contains(new AnyValue[] {
            stringValue("org.neo4j:chevyMakesTheTruck=bobMcCoshMakesTheDifference"),
            stringValue("This is a description"),
            ValueUtils.of(map(attributeName, map("description", "This is the attribute desc.", "value", null)))
        });
    }

    @Test
    void shouldHandleCompositeAttributes() throws Throwable {
        // given
        ObjectName beanName = new ObjectName("org.neo4j:chevyMakesTheTruck=bobMcCoshMakesTheDifference");
        when(jmxServer.queryNames(new ObjectName("*:*"), null)).thenReturn(asSet(beanName));
        when(jmxServer.getMBeanInfo(beanName))
                .thenReturn(new MBeanInfo(
                        "org.neo4j.SomeMBean",
                        "This is a description",
                        new MBeanAttributeInfo[] {
                            new MBeanAttributeInfo(
                                    "name", "differenceMaker", "Who makes the difference?", true, false, false)
                        },
                        null,
                        null,
                        null));
        when(jmxServer.getAttribute(beanName, "name"))
                .thenReturn(new CompositeDataSupport(
                        new CompositeType(
                                "myComposite",
                                "Composite description",
                                new String[] {"key1", "key2"},
                                new String[] {"Can't be empty", "Also can't be empty"},
                                new OpenType<?>[] {SimpleType.STRING, SimpleType.INTEGER}),
                        map("key1", "Hello", "key2", 123)));

        JmxQueryProcedure procedure = new JmxQueryProcedure(new QualifiedName("bob"), jmxServer);

        // when
        RawIterator<AnyValue[], ProcedureException> result =
                procedure.apply(null, new AnyValue[] {stringValue("*:*")}, EMPTY_RESOURCE_TRACKER);

        // then
        assertThat(asList(result)).contains(new AnyValue[] {
            stringValue("org.neo4j:chevyMakesTheTruck=bobMcCoshMakesTheDifference"),
            stringValue("This is a description"),
            ValueUtils.of(map(
                    attributeName,
                    map(
                            "description",
                            "Who makes the difference?",
                            "value",
                            map(
                                    "description",
                                    "Composite description",
                                    "properties",
                                    map("key1", "Hello", "key2", 123)))))
        });
    }

    @Test
    void shouldConvertAllStandardBeansWithoutError() throws Throwable {
        // given
        MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();

        JmxQueryProcedure procedure = new JmxQueryProcedure(new QualifiedName("bob"), jmxServer);

        // when
        RawIterator<AnyValue[], ProcedureException> result =
                procedure.apply(null, new AnyValue[] {stringValue("*:*")}, EMPTY_RESOURCE_TRACKER);

        // then we verify that we respond with the expected number of beans without error
        //      .. we don't assert more than this, this is more of a smoke test to ensure
        //      that independent of platform, we never throw exceptions even when converting every
        //      single MBean into Neo4j types, and we always get the correct number of MBeans out.
        assertThat(asList(result)).hasSize(jmxServer.getMBeanCount());
    }
}
