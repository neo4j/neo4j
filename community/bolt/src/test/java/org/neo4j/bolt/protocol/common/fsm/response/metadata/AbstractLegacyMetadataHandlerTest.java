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
package org.neo4j.bolt.protocol.common.fsm.response.metadata;

import java.util.Collections;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.assertions.ListValueAssertions;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;

public abstract class AbstractLegacyMetadataHandlerTest extends AbstractMetadataHandlerTest {
    @Test
    void shouldApplyNotifications() {
        var notification1 = Mockito.mock(Notification.class);
        var notification2 = Mockito.mock(Notification.class);

        Mockito.doReturn(new InputPosition(5, 42, 3)).when(notification1).getPosition();
        Mockito.doReturn("Neo4j.Test.ThingsHappened").when(notification1).getCode();
        Mockito.doReturn("Something Happened").when(notification1).getTitle();
        Mockito.doReturn("Things may have happened and you have been notified")
                .when(notification1)
                .getDescription();
        Mockito.doReturn(SeverityLevel.WARNING).when(notification1).getSeverity();
        Mockito.doReturn(NotificationCategory.DEPRECATION).when(notification1).getCategory();

        Mockito.doReturn(InputPosition.empty).when(notification2).getPosition();
        Mockito.doReturn("Neo4j.Test.OtherThings").when(notification2).getCode();
        Mockito.doReturn("Something else").when(notification2).getTitle();
        Mockito.doReturn("Oh boy").when(notification2).getDescription();
        Mockito.doReturn(SeverityLevel.INFORMATION).when(notification2).getSeverity();
        Mockito.doReturn(NotificationCategory.HINT).when(notification2).getCategory();

        this.handler.onNotifications(this.consumer, List.of(notification1, notification2), Collections.emptyList());

        var captor = ArgumentCaptor.forClass(ListValue.class);
        Mockito.verify(this.consumer).onMetadata(Mockito.eq("notifications"), captor.capture());

        ListValueAssertions.assertThat(captor.getValue())
                .hasSize(2)
                .satisfies(
                        notification -> Assertions.assertThat(notification)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(6)
                                .containsEntry("code", Values.utf8Value("Neo4j.Test.ThingsHappened"))
                                .containsEntry("title", Values.utf8Value("Something Happened"))
                                .containsEntry(
                                        "description",
                                        Values.utf8Value("Things may have happened and you have been notified"))
                                .containsEntry("severity", Values.utf8Value("WARNING"))
                                .containsEntry("category", Values.utf8Value("DEPRECATION"))
                                .containsEntry("position", position -> Assertions.assertThat(position)
                                        .asInstanceOf(MapValueAssertions.mapValue())
                                        .hasSize(3)
                                        .containsEntry("offset", Values.longValue(5))
                                        .containsEntry("line", Values.longValue(42))
                                        .containsEntry("column", Values.longValue(3))),
                        Index.atIndex(0))
                .satisfies(
                        notification -> Assertions.assertThat(notification)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(5)
                                .containsEntry("code", Values.utf8Value("Neo4j.Test.OtherThings"))
                                .containsEntry("title", Values.utf8Value("Something else"))
                                .containsEntry("description", Values.utf8Value("Oh boy"))
                                .containsEntry("severity", Values.utf8Value("INFORMATION"))
                                .containsEntry("category", Values.utf8Value("HINT"))
                                .doesNotContainKey("position"),
                        Index.atIndex(1));
    }

    @Test
    void shouldOmitNotificationsWhenEmpty() {
        this.handler.onNotifications(this.consumer, Collections.emptyList(), Collections.emptyList());

        Mockito.verifyNoInteractions(this.consumer);
    }
}
