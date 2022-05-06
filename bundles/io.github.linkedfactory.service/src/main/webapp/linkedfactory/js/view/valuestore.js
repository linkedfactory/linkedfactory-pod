define(function() {
	return function() {
		this.loadData = function(items, properties, from, to, limit) {
			var self = this;
			var deferred = $.Deferred();
			$.ajax({
				dataType : "json",
				url : "/linkedfactory/values",
				data : {
					items : items.join(" "),
					properties : properties.join(" "),
					from : from,
					to : to,
					limit : limit
				},
				success : function(loadedData) {
					$.each(Object.keys(loadedData), function(index1, item) {
						var newItemData = loadedData[item];
						if (!newItemData) {
							return;
						}
						$.each(Object.keys(newItemData), function(index2, property) {
							var newPropertyData = newItemData[property];
							if (newPropertyData) {
								// oldest values must come first
								newPropertyData.reverse();
							}
						});
					});
					deferred.resolve(loadedData);
					self.trigger("store-loadedData", loadedData);
				},
				error : deferred.reject
			});
			return deferred;
		}
	};
});