var container = require("vertx/container");
var vertx = require("vertx")
var vertxTests = require("vertx_tests");
var vassert = require("vertx_assert");
var when = require("when");

var eb = vertx.eventBus;

var queueConfig = {address: 'test.myQueue'};
var testJob = {blah: "somevalue"};

var script = this;
container.deployModule(java.lang.System.getProperty("vertx.modulename"), queueConfig, function(err, deployID) {
	vassert.assertNull(err);
	vertxTests.startTests(script);
});

function testStatus()
{
	var notifyJobDoneReplier = null;
	
	function notifyJobDone() {
		notifyJobDoneReplier({});
		return when.resolve();
	}
	
	function sendWork() {
		eb.send(queueConfig.address, testJob);
		return when.resolve();
	}
	
	function registerProcessor() {
		// create a processor
		var processorId = java.util.UUID.randomUUID().toString();
		var handler = function(message, replier) {
		  vassert.assertEquals(JSON.stringify(message), JSON.stringify(testJob));
		  notifyJobDoneReplier = replier;
		};
		eb.registerHandler(processorId, handler);
		
		// register it
		eb.send(queueConfig.address+".register", {processor: processorId});
		return when.resolve();
	}
	
	when(assertStatus({pending: 0, processing: 0}))
		.then(sendWork)
		.then(assertStatus({pending: 1, processing: 0}))
		.then(registerProcessor)
		.then(assertStatus({pending: 0, processing: 1}))
		.then(notifyJobDone)
		.then(assertStatus({pending: 0, processing: 0}))
		.then(function(){vassert.testComplete()});
}

/**
 * Checks whether the work-queue's status keys match those supplied.
 */
function assertStatus(expected) {
	return function() {
		var deferred = when.defer();
		function checkStats(reply) {
			// check the operation succeeded
			vassert.assertEquals("ok", reply.status);
			// check each status property against the expected
			for(var statName in expected) {
				vassert.assertNotNull(reply[statName]);
				var errorMsg = "status '"+statName+"'";
				vassert.assertEquals(errorMsg, expected[statName], reply[statName], 0);
			}
			deferred.resolve();
		}
		eb.send(queueConfig.address+".status", {}, checkStats);
		return deferred.promise;
	}
}