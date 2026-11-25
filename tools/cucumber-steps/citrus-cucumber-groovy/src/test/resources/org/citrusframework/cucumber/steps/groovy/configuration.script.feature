Feature: Configuration script

  Scenario: Inline config
    Given create configuration
    """
    configuration {
        endpoints {
            http {
                server('helloServer') {
                    port = 0
                    autoStart = true
                }
            }
        }
    }
    """
    When verify endpoint helloServer
    And store the port of HTTP server helloServer as serverPort
    Given URL: http://localhost:${serverPort}
    Then send GET /hello
    And receive HTTP 200 OK

  Scenario: Config file resource
    Given load configuration citrus.configuration.groovy
    When verify endpoint hello
    When endpoint hello sends body Hello from new direct endpoint!
    Then endpoint hello should receive body Hello from new direct endpoint!
