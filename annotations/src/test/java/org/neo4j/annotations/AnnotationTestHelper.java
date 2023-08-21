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
package org.neo4j.annotations;

import static java.util.Arrays.copyOf;
import static org.neo4j.annotations.AnnotationConstants.DEFAULT_NEW_LINE;
import static org.neo4j.annotations.AnnotationConstants.WINDOWS_NEW_LINE;

import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.io.InputStream;
import org.neo4j.annotations.api.PublicApiAnnotationProcessor;

public class AnnotationTestHelper {
    /**
     * Looks at contents of a text file and sees whether or not it has windows-style newlines, or unix style.
     * Used in tests to make annotation processors generate files matching signature files to verify contents with.
     * Why not do it the other way around, you ask (i.e. convert the the signature files when reading them)?
     * The assertions framework used in these annotation tests makes that very hard and fragile with the google assertions.
     *
     * @param fullyQualifiedName the resource name to look at.
     * @return either {@link AnnotationConstants#DEFAULT_NEW_LINE} or {@link AnnotationConstants#WINDOWS_NEW_LINE}.
     * @throws IOException on I/O error.
     */
    public static String detectNewLineSignature(String fullyQualifiedName) throws IOException {
        try (InputStream inputStream =
                PublicApiAnnotationProcessor.class.getClassLoader().getResourceAsStream(fullyQualifiedName)) {
            byte[] buffer = new byte[1024];
            int read = inputStream.read(buffer);
            return hasWindowsNewLineSignature(copyOf(buffer, read)) ? WINDOWS_NEW_LINE : DEFAULT_NEW_LINE;
        }
    }

    private static boolean hasWindowsNewLineSignature(byte[] buffer) {
        byte[] windowsNewLineChars = WINDOWS_NEW_LINE.getBytes();
        return Bytes.indexOf(buffer, windowsNewLineChars) != -1;
    }
}
