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
.factory 'NTN', [
  'Settings', '$q', '$rootScope'
  (Settings, $q, $rootScope) ->
    _dfd = null
    loggedIn = no

    loginFrame = angular.element('<iframe>')
    .attr('name', 'neo4jLogin')
    .attr('allowtransparency', true)
    .attr('seamless', true)
    .css(
      'background-color': 'rgba(0, 0, 0, 0)'
      border: '0px none rgba(0, 0, 0, 0)'
      overflow: 'hidden'
      visibility: 'visible'
      margin: '0px'
      padding: '0px'
      '-webkit-tap-highlight-color': 'transparent'
      position: 'fixed'
      left: '0px'
      top: '0px'
      width: '100%'
      height: '100%'
      'z-index': '9999'
      display: 'none'
    )
    .insertAfter('body')

    ajaxFrame = angular.element('<iframe>')
    .attr('name', 'neo4jAjax')
    .css(
      height: '0',
      width: '0',
      visibility: 'hidden',
      display: 'none'
    )
    .insertAfter('body')

    _close = ->
      loginFrame.removeAttr('src').hide()
      if loggedIn then _dfd?.resolve() else _dfd?.reject()
      return

    _ajaxConnect = ->
      ajaxFrame.attr('src', Settings.endpoint.ntn)

    _ajaxDeferred = []
    _ajax = (data) ->
      dfd = $q.defer()
      if not loggedIn
        _ajaxDeferred.push([data, dfd])
        return dfd.promise

      pm({
        target: window.frames['neo4jAjax']
        type:"ajax",
        data: data,
        success: (data) ->
          if(data.status < 300)
            dfd.resolve(data.responseJSON, data)
          else
            dfd.reject(data)
      })
      dfd.promise

    # Recieve messages from the login frame
    # pm.bind 'login.ready', (data) ->

    pm.bind 'login.close', _close
    pm.bind 'ajax.ready', ->
      loggedIn = yes
      $rootScope.$broadcast('user:authenticated', yes)
      while d = _ajaxDeferred.pop()
        _ajax(d[0])
        .then(d[1].resolve, d[1].reject)
      return

    pm.bind 'ajax.failed', ->
      loggedIn = no
      $rootScope.$broadcast('user:authenticated', no)
      d[1].reject({}) for d in _ajaxDeferred
      return

    _ajaxConnect()

    {
      open: ->
        _dfd = $q.defer()
        loginFrame.attr('src', Settings.endpoint.login).show()
        _dfd.promise

      logout: ->
        _dfd = $q.defer()
        loginFrame.attr('src', Settings.endpoint.logout)
        _dfd.promise

      close: _close

      ajax: _ajax

      authenticated: -> loggedIn
    }
]
