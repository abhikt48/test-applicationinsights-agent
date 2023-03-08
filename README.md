# test-applicationinsights-agent
Test project for applicationinsights-agent

# Steps for publish logs to APP Insight
1. Update connection string @ https://github.com/abhikt48/test-applicationinsights-agent/blob/main/agent/applicationinsights.json
2. Dowload and copy `applicationinsights-agent-3.4.10.jar` @ https://github.com/abhikt48/test-applicationinsights-agent/tree/main/agent
3. Update ServiBus, Blob, Queue and MongoDB configuration @ https://github.com/abhikt48/test-applicationinsights-agent/blob/main/src/main/java/com/abhi/test/ai/agent/sbus/storage/TestAiAgentWithMultiTransports.java
4. Run `TestAiAgentWithMultiTransports.java` with only one VM argument `-javaagent:"agent/applicationinsights-agent-3.4.10.jar"`
- Which should start successfully
5. Publish one message to ServiceBus Queue which should be consumed by java Sbus message listener app and publish to Blob, Queue and get document length from MongoDB 
4. Open App Insight to view dependency tree
