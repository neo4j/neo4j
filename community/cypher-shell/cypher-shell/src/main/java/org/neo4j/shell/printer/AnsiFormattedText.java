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
package org.neo4j.shell.printer;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.shell.system.Tty;

/**
 * A piece of text which can be rendered with Ansi format codes.
 * <p>
 * Attributes set one after another are collected and emitted as a single escape sequence when the next piece of
 * text is appended, so {@code colorDefault().boldOff().append("yo")} renders as {@code ESC[39;22myo}.
 */
public class AnsiFormattedText {
    private static final String CSI = "\u001B[";
    private static final int RESET = 0;
    private static final int BOLD = 1;
    private static final int BOLD_OFF = 22;
    private static final int FG_RED = 31;
    private static final int FG_BRIGHT_RED = 91;
    private static final int FG_YELLOW = 33;
    private static final int FG_DEFAULT = 39;

    /** When disabled only plain text is produced, for example when the output is redirected to a file. */
    private static boolean enabled = Tty.isStdoutAndStderrTty();

    private final StringBuilder ansi = new StringBuilder();
    private final StringBuilder plain = new StringBuilder();
    private final List<Integer> pendingAttributes = new ArrayList<>();
    private boolean needsReset = false;

    public static void setEnabled(boolean enabled) {
        AnsiFormattedText.enabled = enabled;
    }

    /** Resets the enabled flag to what {@link AnsiPrinter} would pick for the current process. */
    public static void resetToDefault() {
        enabled = Tty.isStdoutAndStderrTty();
    }

    /** Returns a new empty instance */
    public static AnsiFormattedText s() {
        return new AnsiFormattedText();
    }

    public static AnsiFormattedText from(String string) {
        return new AnsiFormattedText().append(string != null ? string : "");
    }

    private AnsiFormattedText reset() {
        if (needsReset) {
            pendingAttributes.add(RESET);
            needsReset = false;
        }
        return this;
    }

    /** Adds reset and returns the formatted string. */
    public String resetAndRender() {
        reset();
        flushAttributes();
        return ansi.toString();
    }

    /** Returns the text without formatting. */
    public String plainString() {
        return plain.toString();
    }

    /** Append a string using the current formatting */
    public AnsiFormattedText append(CharSequence s) {
        flushAttributes();
        ansi.append(s);
        plain.append(s);
        return this;
    }

    public AnsiFormattedText append(AnsiFormattedText s) {
        flushAttributes();
        ansi.append(s.resetAndRender());
        plain.append(s.plain);
        needsReset = false;
        return this;
    }

    public AnsiFormattedText newLine() {
        return append(System.lineSeparator());
    }

    private void flushAttributes() {
        if (pendingAttributes.isEmpty()) {
            return;
        }
        // A lone reset renders as ESC[m, which is what a terminal expects and shorter than ESC[0m
        boolean plainReset = pendingAttributes.size() == 1 && pendingAttributes.get(0) == RESET;
        ansi.append(CSI);
        if (!plainReset) {
            for (int i = 0; i < pendingAttributes.size(); i++) {
                if (i != 0) {
                    ansi.append(';');
                }
                ansi.append(pendingAttributes.get(i));
            }
        }
        ansi.append('m');
        pendingAttributes.clear();
    }

    private AnsiFormattedText attribute(int attribute) {
        if (enabled) {
            pendingAttributes.add(attribute);
            needsReset = true;
        }
        return this;
    }

    /** Append bold ansi code. */
    public AnsiFormattedText bold() {
        return attribute(BOLD);
    }

    public AnsiFormattedText bold(String bold) {
        return bold().append(bold).boldOff();
    }

    /** Append bold reset ansi code. */
    public AnsiFormattedText boldOff() {
        return attribute(BOLD_OFF);
    }

    /** Append red foreground ansi code. */
    public AnsiFormattedText colorRed() {
        return attribute(FG_RED);
    }

    public AnsiFormattedText colorRed(CharSequence s) {
        return colorRed().append(s).colorDefault();
    }

    /** Append bright red foreground ansi code. */
    public AnsiFormattedText brightRed() {
        return attribute(FG_BRIGHT_RED);
    }

    public AnsiFormattedText brightRed(CharSequence s) {
        return brightRed().append(s).colorDefault();
    }

    /** Append yellow foreground ansi code. */
    public AnsiFormattedText colorOrange() {
        return attribute(FG_YELLOW);
    }

    public AnsiFormattedText orange(String s) {
        return colorOrange().append(s).colorDefault();
    }

    /** Append reset foreground color ansi code. */
    public AnsiFormattedText colorDefault() {
        return attribute(FG_DEFAULT);
    }

    public int textLength() {
        return plain.length();
    }
}
