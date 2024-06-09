# stream-relay
A minimal Pub/Sub implementation in rust

# Serialization Protocol Specs
To communicate with the `Stream-Relay server`, `Stream-Relay clients` use a protocol called **Stream-Relay Serialization Protocol (SESP)**. While the protocol was designed specifically for `Stream-Relay`, you can use it for other client-server software projects.

SESP is a compromise among the following considerations:

* Simple to implement.
* Fast to parse.
* Human readable.

SESP is the protocol you should implement in your Stream-Relay client.

## SESP Rules
* all command transmissions consists of 3 parts as following:

    `<ActionByte><Data><LineTerminator>`

    where:

    **ActionByte:** single byte that signals the type of command.

    **Data:** any valid utf8 encoded string that doesn't contain `LineTerminator`.

    **LineTerminator:** an ASCII newline '\n' to signaling the end of command.

* The following table summarizes the SESP Command types that Stream-Relay supports:

| S.No. | Command | ActionByte | example | description |
|---|---|---|---|---|
| 1. | CreateTopic | `#` | `#foo\n` | this command can be used by admin client inorder to create new topics |
| 2. | DeleteTopic | `!` | `!foo\n` | [not yet supported] this command can be used by admin client inorder to delete an existing topics |
| 3. | SelectTopic | `@` | `@foo\n` | this must be the first command issued by publisher/subscriber client to select topic and can be used only once per connection |
| 4. | SetReadOffset | `$` | `$1001\n` | this command can be used by subscriber client any time to set read offset [default read offset: 0 at the start of session] |
| 5. | ReadMessage | `<` | `<\n` | this command can be used by subscriber client to read message at current read offset and advance read offset by 1 |
| 6. | PublishMessage | `>` | `>hello world\n` | this command can be used by publisher client to publish message to a topic. |

* every command can have either positive (+) or negative (-) response where positive response means success and negative response means Error, response to different commands are summerized as following:

| S.No. | Command | + response example | -ve response example |
|---|---|---|---|
| 1. | CreateTopic | `+\n` | `-AlreadyExists\n` |
| 2. | DeleteTopic | `+\n` | `-NoSuchTopicExists\n` |
| 3. | SelectTopic | `+\n` | `-NoSuchTopicExists\n` |
| 4. | SetReadOffset | `+\n` | `-ReadOffsetCanNotBeNegative\n` |
| 5. | ReadMessage | `+hello world\n` | `-None\n` |
| 6. | PublishMessage | `+\n` | `-IOError\n` |

> **Note:** all types of error are not yet decided and **might change in near future**
