define(['implementations'], function(implementations){

	return {

	    load: function (name, req, load, config) {

			var i, m, toLoad,
				featureInfo = implementations[name],
				opts = {}.toString,
				isArray = function(it){ return opts.call(it) == '[object Array]'; },
				hasMultipleImpls = isArray(featureInfo),
				_load = function (value) { load(value); };

			if(config.isBuild && hasMultipleImpls){
				// In build context, we want all possible
				// implementations included, but we don't
				// want to actually load them, as this will
				// break the whole process (loading the modules
				// will add 'feature!featureName' to the list
				// of already defined modules, thus leading to
				// a conflict when we try to 'register' another
				// module for the same feature).
				for(i=0, m=featureInfo.length; i<m; i++){
					req([featureInfo[i].implementation], _load);
				}

				// We're done here now.
				return;
			}

			if(hasMultipleImpls){
				// We have different implementations available,
				// test for the one to use.
				for(i=0, m=featureInfo.length; i<m; i++){
					if(featureInfo[i].isAvailable()){
						toLoad = featureInfo[i].implementation;
						break;
					}
				}
			}else{
				toLoad = featureInfo;
			}

			req([toLoad], _load);
	    }
	};
});
