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
package org.neo4j.consistency.report;

import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.internal.counts.GBPTreeCountsStore.nodeKey;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
import org.neo4j.annotations.documented.Warning;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.string.Mask;
import org.neo4j.test.InMemoryTokens;

class ConsistencyReporterTest {
    @Nested
    class TestReportLifecycle {
        @Test
        void shouldSummarizeStatisticsAfterCheck() {
            // given
            ConsistencySummaryStatistics summary = mock(ConsistencySummaryStatistics.class);
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport(mock(InconsistencyLogger.class), summary),
                    mock(ConsistencyReporter.ProxyFactory.class),
                    RecordType.PROPERTY,
                    new PropertyRecord(0),
                    NO_MONITOR);

            // then
            verifyNoMoreInteractions(summary);
        }
    }

    @Nested
    class TestAllReportMessages {
        @ParameterizedTest(name = "{0}")
        @MethodSource(value = "org.neo4j.consistency.report.ConsistencyReporterTest#methods")
        void shouldLogInconsistency(ReportMethods methods) throws Exception {
            // given
            Method reportMethod = methods.reportedMethod;
            Method method = methods.method;
            InconsistencyReport report = mock(InconsistencyReport.class);
            ConsistencyReport.Reporter reporter = new ConsistencyReporter(report);

            // when
            ConsistencyReport section = (ConsistencyReport) reportMethod.invoke(reporter, parameters(reportMethod));
            method.invoke(section, parameters(method));

            // then
            if (method.getAnnotation(Warning.class) == null) {
                var verificationMode = atMostOnce();
                verify(report, verificationMode)
                        .error(
                                any(RecordType.class),
                                any(AbstractBaseRecord.class),
                                argThat(expectedFormat()),
                                any(Object[].class));
                verify(report, verificationMode)
                        .error(any(RecordType.class), any(AbstractBaseRecord.class), argThat(expectedFormat()));
            } else {
                var verificationMode = atMostOnce();
                verify(report, verificationMode)
                        .warning(
                                any(RecordType.class),
                                any(AbstractBaseRecord.class),
                                argThat(expectedFormat()),
                                any(Object[].class));
                verify(report, verificationMode)
                        .warning(any(RecordType.class), any(AbstractBaseRecord.class), argThat(expectedFormat()));
            }
        }

        private Object[] parameters(Method method) {
            try {
                Class<?>[] parameterTypes = method.getParameterTypes();
                Object[] parameters = new Object[parameterTypes.length];
                for (int i = 0; i < parameters.length; i++) {
                    parameters[i] = parameter(parameterTypes[i]);
                }
                return parameters;
            } catch (Exception e) {
                throw e;
            }
        }

        private Object parameter(Class<?> type) {
            if (type == RecordType.class) {
                return RecordType.STRING_PROPERTY;
            }
            if (type == NodeRecord.class) {
                return new NodeRecord(0).initialize(false, 2, false, 1, 0);
            }
            if (type == RelationshipRecord.class) {
                RelationshipRecord relationship = new RelationshipRecord(0);
                relationship.setLinks(1, 2, 3);
                return relationship;
            }
            if (type == PropertyRecord.class) {
                return new PropertyRecord(0);
            }
            if (type == PropertyKeyTokenRecord.class) {
                return new PropertyKeyTokenRecord(0);
            }
            if (type == PropertyBlock.class) {
                return new PropertyBlock();
            }
            if (type == RelationshipTypeTokenRecord.class) {
                return new RelationshipTypeTokenRecord(0);
            }
            if (type == LabelTokenRecord.class) {
                return new LabelTokenRecord(0);
            }
            if (type == DynamicRecord.class) {
                return new DynamicRecord(0);
            }
            if (type == NeoStoreRecord.class) {
                return new NeoStoreRecord();
            }
            if (type == TokenScanDocument.class) {
                return new TokenScanDocument(null);
            }
            if (type == IndexEntry.class) {
                return new IndexEntry(
                        IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1))
                                .withName("index")
                                .materialise(1L),
                        new InMemoryTokens(),
                        0);
            }
            if (type == CountsEntry.class) {
                return new CountsEntry(nodeKey(7), 42);
            }
            if (type == IndexDescriptor.class) {
                return IndexPrototype.forSchema(forLabel(2, 3), IndexProviderDescriptor.UNDECIDED)
                        .withName("index")
                        .materialise(1);
            }
            if (type == SchemaRule.class) {
                return simpleSchemaRule();
            }
            if (type == SchemaRecord.class) {
                return new SchemaRecord(42);
            }
            if (type == RelationshipGroupRecord.class) {
                return new RelationshipGroupRecord(0)
                        .initialize(
                                false,
                                1,
                                Record.NULL_REFERENCE.longValue(),
                                Record.NULL_REFERENCE.longValue(),
                                Record.NULL_REFERENCE.longValue(),
                                Record.NULL_REFERENCE.longValue(),
                                Record.NULL_REFERENCE.longValue());
            }
            if (type == long.class) {
                return 12L;
            }
            if (type == CursorContext.class) {
                return NULL_CONTEXT;
            }
            if (type == Object.class) {
                return "object";
            }
            if (type == int.class) {
                return 2;
            }
            if (type == String.class) {
                return "abc";
            }
            if (type == Object[].class) {
                return new Object[] {1, 2};
            }
            if (type == Class.class) {
                return SchemaRule.class;
            }
            throw new IllegalArgumentException(
                    format("Don't know how to provide parameter of type %s", type.getName()));
        }

        private SchemaRule simpleSchemaRule() {
            return new SchemaRule() {
                @Override
                public long getId() {
                    return 0;
                }

                @Override
                public String getName() {
                    return null;
                }

                @Override
                public SchemaRule withName(String name) {
                    return null;
                }

                @Override
                public SchemaDescriptor schema() {
                    return null;
                }

                @Override
                public String userDescription(TokenNameLookup tokenNameLookup) {
                    return null;
                }

                @Override
                public String toString(Mask mask) {
                    return toString();
                }
            };
        }
    }

    private static ArgumentMatcher<String> expectedFormat() {
        return argument -> argument.trim().split(" ").length > 1;
    }

    public static List<ReportMethods> methods() {
        List<ReportMethods> methods = new ArrayList<>();
        for (Method reporterMethod : ConsistencyReport.Reporter.class.getMethods()) {
            Class<?> reportType = reporterMethod.getReturnType();
            for (Method method : reportType.getMethods()) {
                methods.add(new ReportMethods(reporterMethod, method));
            }
        }
        return methods;
    }

    public static class ReportMethods {
        final Method reportedMethod;
        final Method method;

        ReportMethods(Method reportedMethod, Method method) {
            this.reportedMethod = reportedMethod;
            this.method = method;
        }

        @Override
        public String toString() {
            return reportedMethod.getReturnType().getSimpleName() + "#" + method.getName();
        }
    }
}
