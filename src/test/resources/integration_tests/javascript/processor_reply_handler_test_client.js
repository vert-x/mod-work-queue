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

function testWorkQueueWithReplyHandler() {
    var numMessages = 1;
    var count = 0;
    for (var i = 0; i < numMessages; i++) {
        eb.send('test.orderQueue', {
            blah: "somevalue: " + i
        }, function (reply, replyReplier) {
          replyReplier({'wibble': 'eek'}, function(replyReplyReplier) {
              if (++count == numMessages) {
                  vassert.testComplete();
              }
          });
        });
    }
}


var queueConfig = {address: 'test.orderQueue'}
var script = this;
var numProcessors = 10;
vertx.deployModule(java.lang.System.getProperty("vertx.modulename"), queueConfig, 1, function() {
  vertx.deployVerticle("integration_tests/javascript/order_processor_with_reply_handler.js", null, numProcessors, function(depID) {
    if (depID) {
      initTests(script);
    }
  })
});