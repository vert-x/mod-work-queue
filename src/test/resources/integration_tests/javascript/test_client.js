/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

load('vertx.js')
load("vertx_tests.js")

var eb = vertx.eventBus;

function testWorkQueue() {
  var numMessages = 100;

  var count = 0;
  var doneHandler = function() {
    if (++count == numMessages) {
      eb.unregisterHandler("done", doneHandler);
      vassert.testComplete();
    }
  };

  eb.registerHandler("done", doneHandler);

  for (var i = 0; i < numMessages; i++) {
    eb.send('test.orderQueue', {
      blah: "somevalue: " + i
    })
  }
}

var numProcessors = 10;

var queueConfig = {address: 'test.orderQueue'}
var script = this
vertx.deployModule(java.lang.System.getProperty("vertx.modulename"), queueConfig, function(err, deployID) {
  vertx.deployVerticle("integration_tests/javascript/order_processor.js", numProcessors, function(err, depID) {
    if (depID) {
      initTests(script);
    }
  })
});
