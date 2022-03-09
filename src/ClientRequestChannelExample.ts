/*
 * Copyright 2021-2022 the original author or authors.
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

import { RSocketConnector } from "@rsocket/core";
import { WebsocketClientTransport } from "@rsocket/transport-websocket-client";
import { exit } from "process";
import WebSocket from "ws";
import {
  encodeCompositeMetadata,
  encodeRoute,
  encodeWellKnownMetadataHeader,
  WellKnownMimeType,
} from "@rsocket/composite-metadata";
import APPLICATION_JSON = WellKnownMimeType.APPLICATION_JSON;
import MESSAGE_RSOCKET_COMPOSITE_METADATA = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
import MESSAGE_RSOCKET_ROUTING = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
import MESSAGE_RSOCKET_MIMETYPE = WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE;

async function main() {
  const connector = new RSocketConnector({
    setup: {
      dataMimeType: APPLICATION_JSON.string,
      metadataMimeType: MESSAGE_RSOCKET_COMPOSITE_METADATA.string,
      keepAlive: 1000000,
      lifetime: 100000,
      payload: {
        data: null,
        // please supply app metadata
        metadata: encodeCompositeMetadata([
          [
            "message/x.rsocket.application+json",
            Buffer.from(
              JSON.stringify({
                name: "demo-app",
              })
            ),
          ],
        ]),
      },
    },
    transport: new WebsocketClientTransport({
      debug: true,
      url: "ws://localhost:19999",
      wsCreator: (url) => new WebSocket(url) as any,
    }),
  });

  const rsocket = await connector.connect();

  let i = 0;
  let isComplete = false;
  await new Promise((resolve, reject) => {
    const requester = rsocket.requestChannel(
      {
        data: Buffer.from("" + i++),
        metadata: encodeCompositeMetadata([
          [
            MESSAGE_RSOCKET_ROUTING,
            encodeRoute("com.alibaba.user.UserService.recent"),
          ],
          [
            MESSAGE_RSOCKET_MIMETYPE,
            encodeWellKnownMetadataHeader(
              APPLICATION_JSON.identifier,
              APPLICATION_JSON.string.length
            ),
          ],
        ]),
      },
      1,
      isComplete,
      {
        onError: (e) => reject(e),
        onNext: (payload, isComplete) => {
          console.log(
            `payload[data: ${payload.data}; metadata: ${payload.metadata}]|${isComplete}`
          );

          requester.request(1);

          if (isComplete) {
            resolve(payload);
          }
        },
        onComplete: () => {
          resolve(null);
        },
        onExtension: () => {},
        request: (n) => {
          console.log(`request(${n})`);
          setTimeout(() => {
            if (i === 10) isComplete = true;
            requester.onNext(
              {
                data: Buffer.from("" + i++),
              },
              isComplete
            );
            requester.request(1);
          }, 1000);
        },
        cancel: () => {},
      }
    );
  });
}

main().then(() => exit());
