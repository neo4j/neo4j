/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser;

import cypher.feature.parser.matchers.ValueMatcher;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.opencypher.tools.tck.parsing.generated.FeatureResultsLexer;
import org.opencypher.tools.tck.parsing.generated.FeatureResultsParser;

public class ResultsParser
{
    private FeatureResultsParser parser;
    private FeatureResultsLexer lexer;
    private ParseTreeWalker walker;

    ResultsParser()
    {
        this.lexer = new FeatureResultsLexer( new ANTLRInputStream( "" ) );
        lexer.addErrorListener( new ParsingErrorListener() );
        this.parser = new FeatureResultsParser( new CommonTokenStream( lexer ) );
        parser.addErrorListener( new ParsingErrorListener() );
        this.walker = new ParseTreeWalker();
    }

    Object parseParameter( String value, CypherParametersCreator listener )
    {
        lexer.setInputStream( new ANTLRInputStream( value ) );
        walker.walk( listener, parser.value() );
        return listener.parsed();
    }

    ValueMatcher matcherParse( String value, CypherMatchersCreator listener )
    {
        lexer.setInputStream( new ANTLRInputStream( value ) );
        walker.walk( listener, parser.value() );
        return listener.parsed();
    }

}
