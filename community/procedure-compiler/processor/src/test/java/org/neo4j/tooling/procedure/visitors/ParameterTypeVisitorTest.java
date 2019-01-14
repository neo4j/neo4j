/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.tooling.procedure.visitors;

import com.google.testing.compile.CompilationRule;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.testutils.TypeMirrorTestUtils;
import org.junit.Before;
import org.junit.Rule;

import javax.lang.model.type.TypeVisitor;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

public class ParameterTypeVisitorTest extends TypeValidationTestSuite
{

    @Rule
    public CompilationRule compilationRule = new CompilationRule();
    private Types types;
    private TypeMirrorUtils typeMirrorUtils;
    private TypeMirrorTestUtils typeMirrorTestUtils;

    @Before
    public void prepare()
    {
        Elements elements = compilationRule.getElements();
        types = compilationRule.getTypes();
        typeMirrorUtils = new TypeMirrorUtils( types, elements );
        typeMirrorTestUtils = new TypeMirrorTestUtils( compilationRule );
    }

    @Override
    protected TypeVisitor<Boolean,Void> visitor()
    {
        return new ParameterTypeVisitor( types, typeMirrorUtils );
    }

    @Override
    protected TypeMirrorTestUtils typeMirrorTestUtils()
    {
        return typeMirrorTestUtils;
    }
}
