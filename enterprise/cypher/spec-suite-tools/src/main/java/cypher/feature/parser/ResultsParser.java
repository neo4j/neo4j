/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
