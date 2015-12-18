###!
Copyright (c) 2002-2015 "Neo Technology,"
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

'use strict'

angular.module('neo4jApp.controllers')
  .controller 'SysinfoController', [
    '$scope', 'Settings', 'Server', '$timeout'
  ($scope, Settings, Server, $timeout) ->

    $scope.autoRefresh = false

    $scope.sysinfo = {}

    $scope.refresh = () ->

      # kernel info from JMX
      $scope.sysinfo.kernel ?= {}
      Server.jmx(
        [
          "org.neo4j:instance=kernel#0,name=Configuration"
          "org.neo4j:instance=kernel#0,name=Kernel"
          "org.neo4j:instance=kernel#0,name=Store file sizes"
        ]).success((response) ->
          for r in response
            for a in r.attributes
              $scope.sysinfo.kernel[a.name] = a.value
        ).error((r)-> $scope.sysinfo.kernel = {})

      # primitive counts from JMX
      $scope.sysinfo.primitives ?= {}
      Server.jmx(
        [
          "org.neo4j:instance=kernel#0,name=Primitive count"
        ]).success((response) ->
          for r in response
            for a in r.attributes
              $scope.sysinfo.primitives[a.name] = a.value
        ).error((r)-> $scope.sysinfo.primitives = {})

      # page cache data from JMX
      $scope.sysinfo.cache ?= { available: false }
      Server.jmx(
        [
          "org.neo4j:instance=kernel#0,name=Page cache"
        ]).success((response) ->
          for r in response
            $scope.sysinfo.cache.available = true
            for a in r.attributes
              $scope.sysinfo.cache[a.name] = a.value
        ).error((r)-> $scope.sysinfo.cache = { available: false })

      # transactions from JMX
      $scope.sysinfo.tx ?= { available: false }
      Server.jmx(
        [
          "org.neo4j:instance=kernel#0,name=Transactions"
        ]).success((response) ->
          for r in response
            $scope.sysinfo.tx.available = true
            for a in r.attributes
              $scope.sysinfo.tx[a.name] = a.value
        ).error((r)-> $scope.sysinfo.tx = {available: false})

      # transactions from JMX
      $scope.sysinfo.ha ?= { }
      Server.jmx(
        [
          "org.neo4j:instance=kernel#0,name=High Availability"
        ]).success((response) ->
          for r in response
            $scope.sysinfo.ha.clustered = true
            for a in r.attributes
              if a.name is "InstancesInCluster"
                $scope.sysinfo.ha.ClusterMembers = {}
                for member in a.value
                  clusterMember = {}
                  for ma in member.value
                    clusterMember[ma.name] = ma.value
                  clusterMember.connected = false
                  $scope.sysinfo.ha.ClusterMembers[clusterMember.instanceId] = clusterMember
              else
                if a.name is "InstanceId"
                  connectedMemberId = a.value
                $scope.sysinfo.ha[a.name] = a.value
            $scope.sysinfo.ha.ClusterMembers[connectedMemberId].connected = true
        ).error((r)-> $scope.sysinfo.ha = { clustered: false })

    timer = null
    refreshLater = () =>
      $timeout.cancel(timer)

      if $scope.autoRefresh
        $scope.refresh()
        timer = $timeout(
          refreshLater
          ,
          (Settings.refreshInterval * 1000)
        )

    $scope.isMaster = (member) ->
      return member.haRole is 'master'

    $scope.toggleAutoRefresh = () ->
      $scope.autoRefresh = !$scope.autoRefresh
      refreshLater()

    $scope.refresh()
]
