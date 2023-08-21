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
package org.neo4j.bolt.testing.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.bolt.negotiation.ProtocolVersion;

/**
 * Defines a range of protocol versions.
 * <p/>
 * At minimum, a version range includes a {@link #major()} component (thus including or excluding all supported
 * protocol versions within that major revision). It may, however, also include a {@link #minor()} and
 * {@link #range()} component in order to include or exclude a specific minor revision or range of minor revisions.
 * <p/>
 * The {@link #range()} component hereby follows the specification outlined by {@link ProtocolVersion}.
 * <p />
 * This a meta-annotation.
 */
@Documented
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Version {

    /**
     * Selects a major version which is matched by this specification.
     *
     * @return a major version number.
     */
    int major();

    /**
     * Selects a minor version which is matched by this specification.
     * <p/>
     * When {@code -1} is specified, all revisions of the specified {@link #major()} version are considered.
     *
     * @return a minor version number.
     */
    int minor() default -1;

    /**
     * Selects a range of versions which shall be matched by this specification.
     * <p/>
     * Only applicable when {@link #minor()} is set. This property operates the same way as its counterpart within
     * the protocol negotiation. As such, it will <em>subtract</em> from the {@link #minor()} version component.
     *
     * @return a range of versions.
     */
    int range() default 0;
}
