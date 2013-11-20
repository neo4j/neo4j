/**
 * SyntaxHighlighter
 * http://alexgorbatchev.com/SyntaxHighlighter
 *
 * SyntaxHighlighter is donationware. If you are using it, please donate.
 * http://alexgorbatchev.com/SyntaxHighlighter/donate.html
 *
 * @version
 * 3.0.83 (July 02 2010)
 * 
 * @copyright
 * Copyright (C) 2004-2010 Alex Gorbatchev.
 *
 * @license
 * Dual licensed under the MIT and GPL licenses.
 * 
 * Modified by the Neo4j Team.
 */
;(function()
{
  // CommonJS
  typeof(require) != 'undefined' ? SyntaxHighlighter = require('shCore').SyntaxHighlighter : null;

  function Brush()
  {
        var funcs   =   'abs acos all allShortestPaths any asin atan atan2 avg ceil coalesce collect cos cot count degrees e endnode exp extract filter floor has haversin head id labels last left length log log10 lower ltrim max min node nodes none percentileCont percentileDisc pi radians rand range reduce rel relationship relationships replace right round rtrim shortestPath sign sin single sqrt startnode stdev stdevp str substring sum tail tan timestamp trim type upper';

        var keywords =  'as asc ascending assert by case constraint create cypher delete desc descending distinct drop else end false foreach in index is limit match merge null on optional order remove return scan set skip start then true union unique using when where with';

        var operators = 'or and not xor';

    this.regexList = [
      { regex: SyntaxHighlighter.regexLib.singleLineCComments,  css: 'comments' },    // one line comments
      { regex: SyntaxHighlighter.regexLib.multiLineDoubleQuotedString,  css: 'string' },      // double quoted strings
      { regex: SyntaxHighlighter.regexLib.multiLineSingleQuotedString,  css: 'string' },      // single quoted strings
      { regex: new RegExp(this.getKeywords(funcs), 'gmi'),        css: 'color2' },      // functions
      { regex: new RegExp(this.getKeywords(operators), 'gmi'),      css: 'color1' },      // operators and such
      { regex: new RegExp(this.getKeywords(keywords), 'gmi'),     css: 'keyword' }      // keyword
      ];
  };

  Brush.prototype = new SyntaxHighlighter.Highlighter();
  Brush.aliases = ['cypher'];

  SyntaxHighlighter.brushes.Cypher = Brush;

  // CommonJS
  typeof(exports) != 'undefined' ? exports.Brush = Brush : null;
})();

