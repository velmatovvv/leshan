# CoAP over TLS with PSK

The experimental java-coap TCP transport supports TLS 1.2 with pre-shared keys.
It uses Bouncy Castle's TLS implementation because standard Java JSSE does not
support PSK cipher suites. Both ends offer `TLS_DHE_PSK_WITH_AES_128_GCM_SHA256`
(preferred, with forward secrecy) `TLS_PSK_WITH_AES_128_GCM_SHA256` and `TLS_PSK_WITH_AES_128_CCM_8`
(the PSK cipher required by RFC 8323).

## Server

Use `JavaCoapsTcpServerEndpointsProvider` and store client credentials in the
usual Leshan `SecurityStore`:

```java
securityStore.add(SecurityInfo.newPreSharedKeyInfo(endpointName, pskIdentity, pskKey));

JavaCoapsTcpServerEndpointsProvider provider =
        new JavaCoapsTcpServerEndpointsProvider(new InetSocketAddress(5684), true);
```

Pass the provider to the Leshan server builder as usual. The second constructor
argument selects a PSK-only endpoint, including when the server has certificates.
The original constructor automatically selects PSK when no server private key is
configured, and retains the existing X.509 behavior when a private key is present.
PSK and X.509 require separate listening endpoints to be used at the same time.

PSK identities are UTF-8 strings and keys are bytes. Unknown identities and wrong
keys fail the handshake. The authenticated identity becomes a Leshan `PskIdentity`,
so normal endpoint authorization applies. Removing credentials from an editable
security store closes matching connections. PSK session resumption is disabled;
a new connection always verifies credentials against the current store.

## Client

Use `JavaCoapsTcpClientEndpointsProvider` with a `coaps+tcp://` server URI and
Security Object mode `SecurityMode.PSK`. The client selects PSK automatically and
uses the configured PSK identity and key. Certificate mode continues to use JSSE.
Both client and server limit the PSK handshake to ten seconds.

## Validation

`PskTlsTest` covers registration, authenticated identity, server-initiated reads,
deregistration, reconnect, unknown identity, wrong key and credential removal.
Run it with:

```sh
mvn -pl leshan-integration-tests -am test -Dtest=PskTlsTest -Dsurefire.failIfNoSpecifiedTests=false
```
