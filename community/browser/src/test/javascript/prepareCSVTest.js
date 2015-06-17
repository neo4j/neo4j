/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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


/**
 This file is for exporting data in CSV format into an outfile
 "/tmp/csv-export-javascript.csv" that could be read by other
 modules to test it's format.

 You have to include helpers.js and serializer.js before this file
 for it to work.

 e.g. `cat .tmp/lib/helpers.js .tmp/lib/serializer.js src/test/javascript/prepareCSVTest.js | node`
 */

var fs = require('fs');
var s = new neo.serializer();

s.columns(['col1', 'col2', 'col3']);
s.append(['Hello "Mr. Quote"', null, '']);
var out = s.output();

fs.writeFile("/tmp/csv-export-javascript.csv", out, function(err) {
  if(err) {
    throw err;
  }
});
