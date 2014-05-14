angular.module('neo4jApp')
.run [
  'NTN'
  'localStorageService'
  '$rootScope'
  (NTN, localStorageService, $rootScope) ->
    # Sync local storage to the cloud
    sync = ->
      keys = localStorageService.keys()
      d = {}
      d[k] = localStorageService.get(k) for k in keys

      NTN.ajax({
        contentType: 'application/json'
        method: 'PUT'
        url: '/api/v1/store'
        data: JSON.stringify(d)
      }).then((response)->
        for k, v of response
          localStorageService.set(k, v)
        $rootScope.$broadcast 'localStorage:update'
      )

    $rootScope.$on 'user:authenticated', (evt, authenticated) ->
      if authenticated
        NTN.ajax('/api/v1/me')
        .then(
          (data) ->
            $rootScope.currentUser = data
            sync()
        ,
          ->
            $rootScope.currentUser = undefined
        )
      else
        $rootScope.currentUser = undefined

]
