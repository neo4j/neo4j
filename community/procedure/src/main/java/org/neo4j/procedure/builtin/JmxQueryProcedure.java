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

import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class JmxQueryProcedure extends CallableProcedure.BasicProcedure {
    private final MBeanServer jmxServer;

    public JmxQueryProcedure(QualifiedName name, MBeanServer jmxServer) {
        super(procedureSignature(name)
                .in("query", Neo4jTypes.NTString)
                .out("name", Neo4jTypes.NTString)
                .out("description", Neo4jTypes.NTString)
                .out("attributes", Neo4jTypes.NTMap)
                .mode(Mode.DBMS)
                .description(
                        "Query JMX management data by domain and name. For instance, use `*:*` to find all JMX beans.")
                .systemProcedure()
                .build());
        this.jmxServer = jmxServer;
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
        String query = getParameter(0, TextValue.class, "query", input).stringValue();
        try {
            // Find all beans that match the query name pattern
            Iterator<ObjectName> names =
                    jmxServer.queryNames(new ObjectName(query), null).iterator();

            // Then convert them to a Neo4j type system representation
            return ResourceRawIterator.from(() -> {
                if (!names.hasNext()) {
                    return null;
                }

                ObjectName name = names.next();
                try {
                    MBeanInfo beanInfo = jmxServer.getMBeanInfo(name);
                    return new AnyValue[] {
                        stringValue(name.getCanonicalName()),
                        stringValue(beanInfo.getDescription()),
                        toNeo4jValue(name, beanInfo.getAttributes())
                    };
                } catch (JMException e) {
                    throw new ProcedureException(
                            Status.General.UnknownError,
                            e,
                            "JMX error while accessing `%s`, please report this. Message was: %s",
                            name,
                            e.getMessage());
                }
            });
        } catch (MalformedObjectNameException e) {
            throw new ProcedureException(
                    Status.Procedure.ProcedureCallFailed,
                    "'%s' is an invalid JMX name pattern. Valid queries should use "
                            + "the syntax outlined in the javax.management.ObjectName API documentation."
                            + "For instance, use '*:*' to find all JMX beans.",
                    query);
        }
    }

    private MapValue toNeo4jValue(ObjectName name, MBeanAttributeInfo[] attributes) throws JMException {
        MapValueBuilder out = new MapValueBuilder();
        for (MBeanAttributeInfo attribute : attributes) {
            if (attribute.isReadable()) {
                out.add(attribute.getName(), toNeo4jValue(name, attribute));
            }
        }
        return out.build();
    }

    private MapValue toNeo4jValue(ObjectName name, MBeanAttributeInfo attribute) throws JMException {
        AnyValue value;
        try {
            value = toNeo4jValue(jmxServer.getAttribute(name, attribute.getName()));
        } catch (RuntimeMBeanException e) {
            if (e.getCause() != null && e.getCause() instanceof UnsupportedOperationException) {
                // We include the name and description of this attribute still - but the value of it is
                // unknown. We do this rather than rethrow the exception, because several MBeans built into
                // the JVM will throw exception on attribute access depending on their runtime state, even
                // if the attribute is marked as readable. Notably the GC beans do this.
                value = NO_VALUE;
            } else {
                throw e;
            }
        }
        MapValueBuilder builder = new MapValueBuilder(2);
        builder.add("description", stringValue(attribute.getDescription()));
        builder.add("value", value);
        return builder.build();
    }

    private AnyValue toNeo4jValue(Object attributeValue) {
        // These branches as per {@link javax.management.openmbean.OpenType#ALLOWED_CLASSNAMES_LIST}
        if (isSimpleType(attributeValue)) {
            return ValueUtils.of(attributeValue);
        } else if (attributeValue.getClass().isArray()) {
            if (isSimpleType(attributeValue.getClass().getComponentType())) {
                return ValueUtils.of(attributeValue);
            } else {
                return toNeo4jValue((Object[]) attributeValue);
            }
        } else if (attributeValue instanceof CompositeData) {
            return toNeo4jValue((CompositeData) attributeValue);
        } else if (attributeValue instanceof ObjectName) {
            return stringValue(((ObjectName) attributeValue).getCanonicalName());
        } else if (attributeValue instanceof TabularData) {
            return toNeo4jValue((Map<?, ?>) attributeValue);
        } else if (attributeValue instanceof Date) {
            return longValue(((Date) attributeValue).getTime());
        } else {
            // Don't convert objects that are not OpenType values
            return NO_VALUE;
        }
    }

    private MapValue toNeo4jValue(Map<?, ?> attributeValue) {
        // Build a new map with the same keys, but each value passed
        // through `toNeo4jValue`
        MapValueBuilder builder = new MapValueBuilder();
        attributeValue.forEach((key, value) -> builder.add(key.toString(), toNeo4jValue(value)));
        return builder.build();
    }

    private ListValue toNeo4jValue(Object[] array) {
        return Arrays.stream(array).map(this::toNeo4jValue).collect(ListValueBuilder.collector());
    }

    private MapValue toNeo4jValue(CompositeData composite) {
        MapValueBuilder properties = new MapValueBuilder();
        for (String key : composite.getCompositeType().keySet()) {
            properties.add(key, toNeo4jValue(composite.get(key)));
        }

        MapValueBuilder out = new MapValueBuilder();
        out.add("description", stringValue(composite.getCompositeType().getDescription()));
        out.add("properties", properties.build());

        return out.build();
    }

    private static boolean isSimpleType(Object value) {
        return value == null || isSimpleType(value.getClass());
    }

    private static boolean isSimpleType(Class<?> cls) {
        return String.class.isAssignableFrom(cls)
                || Number.class.isAssignableFrom(cls)
                || Boolean.class.isAssignableFrom(cls)
                || cls.isPrimitive();
    }
}
