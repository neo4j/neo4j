###
Copyright (c) 2002-2018 "Neo Technology,"
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

define ['ribcage/security/HtmlEscaper','ribcage/Model'], (HtmlEscaper, Model) ->
  
  htmlEscaper = new HtmlEscaper

  class Property extends Model
    
    defaults :
      key        : ""
      value      : ""
      keyError   : false
      valueError : false

    getLocalId : =>
      @get "localId"

    getKey : () =>
      @get "key"

    getValue : () =>
      @get "value"

    getValueError : =>
      @get "valueError"

    getKeyError : =>
      @get "keyError"

    getValueAsJSON : () =>
      if @hasValueError() then @getValue() else JSON.stringify(@getValue())

    getValueAsHtml : () =>
      htmlEscaper.escape @getValueAsJSON()

    getTruncatedHtmlValue : (maxLength=100) =>
      str = @getValueAsJSON()
      if str.length > maxLength
        str = str.substr(0,maxLength-3) + ".."
      htmlEscaper.escape str

    getKeyAsHtml : () =>
      htmlEscaper.escape @getKey()

    setKeyError : (error) =>
      @set "keyError" : error

    setValueError : (error) =>
      @set "valueError" : error  

    setValue : (value) =>
      @set "value" : value

    setKey : (key) =>
      @set "key" : key

    hasKeyError : =>
      @getKeyError() != false

    hasValueError : =>
      @getValueError() != false
    
