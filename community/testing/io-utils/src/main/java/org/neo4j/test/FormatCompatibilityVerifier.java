/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.tags.MultiVersionedTag;
import org.neo4j.test.utils.TestDirectory;

/**
 * A little trick to automatically tell whether or not store format was changed without
 * incrementing the format version. This is done by keeping a zipped store file which is opened and tested on.
 * On failure this test will fail saying that the format version needs update and also update the zipped
 * store with the new version.
 */
@TestDirectoryExtension
public abstract class FormatCompatibilityVerifier {
    @Inject
    private TestDirectory globalDir;

    @Inject
    protected FileSystemAbstraction globalFs;

    @Test
    @MultiVersionedTag
    public void shouldDetectFormatChange() throws Throwable {
        Path storeFile = globalDir.file(storeFileName());
        doShouldDetectFormatChange(zipName(), storeFile);
    }

    protected abstract String zipName();

    protected abstract String storeFileName();

    protected abstract void createStoreFile(Path storeFile) throws IOException;

    protected abstract void verifyFormat(Path storeFile) throws IOException, FormatViolationException;

    protected abstract void verifyContent(Path storeFile) throws IOException;

    private void doShouldDetectFormatChange(String zipName, Path storeFile) throws Throwable {
        try {
            ZipUtils.unzipResource(getClass(), zipName, storeFile);
        } catch (NoSuchFileException e) {
            // First time this test is run, eh?
            createStoreFile(storeFile);
            ZipUtils.zip(globalFs, storeFile, globalDir.file(zipName));
            tellDeveloperToCommitThisFormatVersion(zipName);
        }
        assertTrue(globalFs.fileExists(storeFile), zipName + " seems to be missing from resources directory");

        // Verify format
        try {
            verifyFormat(storeFile);
        } catch (FormatViolationException e) {
            // Good actually, or?
            assertThat(e.getMessage()).contains("format version");

            globalFs.deleteFile(storeFile);
            createStoreFile(storeFile);
            ZipUtils.zip(globalFs, storeFile, globalDir.file(zipName));

            tellDeveloperToCommitThisFormatVersion(zipName);
        }

        // Verify content
        try {
            verifyContent(storeFile);
        } catch (Throwable t) {
            throw new AssertionError(
                    "If this is the single failing test in this component then this failure is a strong indication that format "
                            + "has changed without also incrementing format version(s). Please make necessary format version changes.",
                    t);
        }
    }

    private void tellDeveloperToCommitThisFormatVersion(String zipName) {
        fail(String.format(
                "This is merely a notification to developer. Format has changed and its version has also "
                        + "been properly incremented. A store file with this new format has been generated and should be committed. "
                        + "Please move the newly created file to correct resources location using command:%n"
                        + "mv \"%s\" \"%s\"%n"
                        + "replacing the existing file there",
                globalDir.file(zipName),
                "<corresponding-module>" + pathify(".src.test.resources.")
                        + pathify(getClass().getPackage().getName() + ".") + zipName));
    }

    private static String pathify(String name) {
        return name.replace('.', File.separatorChar);
    }

    public static class FormatViolationException extends Exception {
        public FormatViolationException(Throwable cause) {
            super(cause);
        }

        public FormatViolationException(String message) {
            super(message);
        }
    }
}
