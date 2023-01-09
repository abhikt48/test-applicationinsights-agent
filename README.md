# test-applicationinsights-agent
Test project for applicationinsights-agent

# Steps for publish logs to APP Insight
1. Update connection string @ https://github.com/abhikt48/test-applicationinsights-agent/blob/main/src/main/resources/applicationinsights.json
2. Dowload and copy `applicationinsights-agent-3.4.7.jar` @ https://github.com/abhikt48/test-applicationinsights-agent/tree/main/src/main/resources
3. Run `AiAgentWithSlf4jTest.java` with only one VM argument `-javaagent:"<path>\test-applicationinsights-agent\src\main\resources\applicationinsights-agent-3.4.7.jar"`
- Which should publish/upload logs to APP Insight successfully
4. Run `AiAgentWithJclOverSlf4jTest.java` with only one VM argument `-javaagent:"<path>\test-applicationinsights-agent\src\main\resources\applicationinsights-agent-3.4.7.jar"`
- Failed to publish/upload logs to APP Insight
