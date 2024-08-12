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
package org.neo4j.memory;

import static java.lang.String.format;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

public class MemoryLimitExceededException extends RuntimeException implements Status.HasStatus, HasGqlStatusInfo {
    private final Status status;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public MemoryLimitExceededException(long allocation, long limit, long current, Status status, String settingName) {
        super(getMessage(allocation, limit, current, settingName));
        this.status = status;

        this.gqlStatusObject = null;
        this.oldMessage = getMessage(allocation, limit, current, settingName);
    }

    public MemoryLimitExceededException(
            ErrorGqlStatusObject gqlStatusObject,
            long allocation,
            long limit,
            long current,
            Status status,
            String settingName) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, getMessage(allocation, limit, current, settingName)));
        this.gqlStatusObject = gqlStatusObject;

        this.status = status;
        this.oldMessage = getMessage(allocation, limit, current, settingName);
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return status;
    }

    private static String getMessage(long allocation, long limit, long current, String settingName) {
        if (StringUtils.isEmpty(settingName)) {
            return format(
                    "The allocation of an extra %s would use more than the limit %s. Currently using %s.",
                    humanReadableByteCountBin(allocation),
                    humanReadableByteCountBin(limit),
                    humanReadableByteCountBin(current));
        }

        return format(
                "The allocation of an extra %s would use more than the limit %s. Currently using %s. %s threshold reached",
                humanReadableByteCountBin(allocation),
                humanReadableByteCountBin(limit),
                humanReadableByteCountBin(current),
                settingName);
    }

    private static String humanReadableByteCountBin(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
