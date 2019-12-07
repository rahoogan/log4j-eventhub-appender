# Azure EventHub log4j Appender
A log4j appender which forwards logs straight to Azure EventHub as events. Currently uses the REST API provided by Microsoft [here](https://docs.microsoft.com/en-us/rest/api/eventhub/) to asynchronously forward events.

## Installation

Currently the only method available is to compile from source:

1. Clone this project, and install `maven`

2. Compile the project into a single jar using `maven`:

```bash
mvn clean compile assembly:single
```

3. You should now have a `jar` you can use in your project:

```bash
log4j-eventhub-appender-X.X-SNAPSHOT-jar-with-dependencies.jar
```

## Usage

Assuming you have set up your EventHub destination along with SAS (Shared Access Keys) for authentication, you can configure log4j as follows:

1. Create a `config.properties` file in your project classpath with the properties shown below:

| Parameter         | Required          | Default       | Description                   |
|-------------------|-------------------|---------------|-------------------------------|
| namespacename     | Yes               |               | Azure EventHub Namespace Name |
| eventhubname      | Yes               |               | Azure EventHub Name           |
| saskeyname        | Yes               |               | Azure EventHub Shared Access Key Name which has been granted permission to read/write to the specified EventHub |
| retries           | No                | 0             | Number of times to retry send (HTTP Post) requests on failure (Any response other than 2xx) |

An example is shown below:

```
namespacename=my-eventhub-namespace
eventhubname=my-eventhub-name
saskeyname=my-sas-key-name
saskey=my-sas-key
retries=0
```

2. Ensure that the `jar` built in the installation step is present in your dependencies

3. Configure your `log4j.properties` file to use the appender:

```
# base logger
log4j.rootCategory=INFO,eventhub
log4j.appender.eventhub=com.rahoogan.log4j.EventHubRestAppender
log4j.appender.eventhub.layout.ConversionPattern=%m%n
```

## License
Licensed under the [MIT License](https://github.com/rahoogan/log4j-eventhub-appender/blob/master/LICENSE)
