###!
Copyright (c) 2002-2014 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

'use strict';

angular.module('neo4jApp.services')
.factory 'AsciiTable', () ->
  class AsciiTable
    constructor: () ->
      @max_original_col_width = 0

    get: (data, options = {}) ->
      @data = @prepareData(data)
      @options = options
      @max_column_width = @options?.max_column_width || 30

      @columns = @getColumns(@options.columns)
      tbl = @generateTable()

    reloadWithOptions: (options) ->
      return unless @data
      for k, v of options 
        @options[k] = v
      @get(@data, @options)

    pad: (text = "", length) ->
      "#{text}" + new Array(Math.max((length - ("" + text).length) + 1,0)).join(" ")

    getColumns: (columns = []) ->
      columns ||= Object.keys(@data)
      cols = []

      for name, i in columns
        width = @getColWidth(i, name.length)
        cols.push {index: i, name: name, width: width}
      cols

    prepareData: (data) ->
      new_data = data.map((item) ->
        if Array.isArray(item)
          tmp = item.map((subitem) ->
            if Array.isArray subitem
              subitem = '[' + subitem.join(", ") + ']'
            if typeof subitem in ['number', 'string']
              return {'-': subitem }
            return subitem
          )
          tmp
      )
      new_data

    getColWidth: (col_index, min_length) ->
      width = Math.max(10, min_length)
      for row in @data
        col = row[col_index]
        for k, v of col
          key_prefix_length = if (""+k) is '-' then 0 else (""+k).length+2
          @max_original_col_width = Math.max(@max_original_col_width, key_prefix_length + (""+v).length)
          width = Math.min(Math.max(width, key_prefix_length + (""+v).length), @max_column_width)
      width

    getRowNumberOfLines: (rowdata) ->
      max_lines = 1
      for coldata in rowdata
        max_lines = Math.max(max_lines, Object.keys(coldata).length)
      max_lines

    wrapLongLines: (rowdata) ->
      new_row = []
      for data_column in rowdata
        tmp = {}
        continue if typeof data_column is 'undefined'
        keys = Object.keys(data_column)
        for k, v of data_column
          property_len = (""+k).length + (""+v).length
          property_len = property_len + 2 unless k == '-'
          if property_len <= @max_column_width
            tmp[k] = v
          else
            num_lines = Math.ceil(property_len/@max_column_width)
            chars_taken = 0
            for i in [1..num_lines] by 1
              if i < 2
                take_chars = @max_column_width
                take_chars = take_chars - (""+k).length - 2 unless k == '-'
                tmp[k] = (""+v).substring(0, take_chars)
              else
                take_chars = Math.min((""+v).length-chars_taken, @max_column_width)
                tmp["-"+k+i] = (""+v).substring(chars_taken, chars_taken+take_chars)
              chars_taken += take_chars
        new_row.push tmp
      new_row

    getColNumberOfLines: (coldata) ->
      Math.max(1, Object.keys(coldata).length)

    generateTable: ->
      that = @
      out = []
      thin_line = "+-" + @columns.map((col, index) -> new Array(col.width).join("-")).join('+-') + "+"
      thick_line = "+=" + @columns.map((col, index) -> new Array(col.width).join("=")).join('+=') + "+"
      header = [""].concat(@columns.map((col) -> that.pad(col.name, col.width))).concat([""]).join('|')
      out.push(thick_line)
      out.push(header)
      out.push(thick_line)

      rows = []
      for row in @data
        row = @wrapLongLines(row)
        num_lines = @getRowNumberOfLines(row)
        line_data = []
        lines = []
        line_number = 0
        while line_number < num_lines
          line_data[line_number] ||= []
          for data_column, index in row
            col = @columns[index]
            keys = Object.keys(data_column)
            if not keys[line_number]
              line_data[line_number].push @pad('', col.width)
            else
              line_prefix = if keys[line_number].charAt(0) is '-' then '' else "#{keys[line_number]}: "
              line_data[line_number].push @pad("#{line_prefix}#{data_column[keys[line_number]]}", col.width)
          lines.push '|' + line_data[line_number].join('|') + '|'
          line_number++
        rows.push lines.join("\n")        
        rows.push(thin_line)
      out.push rows.join("\n")
      out.join("\n")

  AsciiTable