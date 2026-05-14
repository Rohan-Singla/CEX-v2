import "dotenv/config";
import { createClient } from "redis";
import { env } from "./utils/env.js";

import {
  BALANCES,
  ORDERBOOKS,
  ORDERS,
  type CreateOrderInput,
  type OrderRecord,
} from "./store/exchange-store.js";

export type EngineCommandType =
  | "create_order"
  | "get_user_balance"
  | "get_order"
  | "cancel_order"
  | "get_depth";

export interface EngineRequest {
  correlationId: string;
  responseQueue: string;
  type: EngineCommandType;
  payload: Record<string, unknown>;
}

export interface EngineResponse {
  correlationId: string;
  ok: boolean;
  data?: unknown;
  error?: string;
}

const brokerClient = createClient({
  url: env.redisUrl,
}).on("error", (error) => {
  console.error("Redis broker client error", error);
});

const responseClient = createClient({
  url: env.redisUrl,
}).on("error", (error) => {
  console.error("Redis response client error", error);
});

await Promise.all([
  brokerClient.connect(),
  responseClient.connect(),
]);

async function sendResponse(
  responseQueue: string,
  response: EngineResponse
): Promise<void> {

  await responseClient.lPush(
    responseQueue,
    JSON.stringify(response)
  );
}

function seedBalance(userId: string): void {

  const existingBalance =
    BALANCES.get(userId);

  if (existingBalance) {
    return;
  }

  BALANCES.set(userId, {
    USD: {
      available: 10000,
      locked: 0,
    },

    BTC: {
      available: 5,
      locked: 0,
    },
  });
}

function createOrder(
  input: CreateOrderInput
): OrderRecord {

  seedBalance(input.userId);

  const order: OrderRecord = {
    orderId: crypto.randomUUID(),

    userId: input.userId,

    type: input.type,

    side: input.side,

    symbol: input.symbol,

    price: input.price,

    qty: input.qty,

    filledQty: 0,

    status: "open",

    fills: [],

    createdAt: Date.now(),
  };

  ORDERS.set(order.orderId, order);


  return order;
}

function cancelOrder(
  orderId: string
): OrderRecord {

  const order = ORDERS.get(orderId);

  if (!order) {
    throw new Error("Order not found");
  }

  if (order.status === "cancelled") {
    throw new Error("Order already cancelled");
  }

  order.status = "cancelled";

  return order;
}

// REMOVED THE ORDER SO NO POINT OF DEPTH FOR NOW 

// function getDepth(symbol: string) {

//   const bids: {x
//     price: number;
//     qty: number;
//   }[] = [];

//   const asks: {
//     price: number;
//     qty: number;
//   }[] = [];

//   for (const order of ORDERS.values()) {

//     if (order.symbol !== symbol) {
//       continue;
//     }

//     if (order.status !== "open") {
//       continue;
//     }

//     if (order.side === "buy") {

//       bids.push({
//         price: order.price || 0,
//         qty: order.qty,
//       });

//     } else {

//       asks.push({
//         price: order.price || 0,
//         qty: order.qty,
//       });
//     }
//   }

//   bids.sort((a, b) => b.price - a.price);

//   asks.sort((a, b) => a.price - b.price);

//   return {
//     symbol,
//     bids,
//     asks,
//   };
// }

function handleEngineRequest(
  message: EngineRequest
): unknown {

  if (message.type === "create_order") {

    const input =
      message.payload as unknown as CreateOrderInput;

    return createOrder(input);

  } else if (message.type === "get_order") {

    const { orderId } =
      message.payload as {
        orderId: string;
      };

    const order = ORDERS.get(orderId);

    if (!order) {
      throw new Error("Order not found");
    }

    return order;

  } else if (
    message.type === "cancel_order"
  ) {

    const { orderId } =
      message.payload as {
        orderId: string;
      };

    return cancelOrder(orderId);

  } else if (
    message.type === "get_depth"
  ) {

    const { symbol } =
      message.payload as {
        symbol: string;
      };

      return {asset : symbol}

    // return getDepth(symbol);

  } else {

    const { userId } =
      message.payload as {
        userId: string;
      };

    seedBalance(userId);

    return BALANCES.get(userId);
  }
}

console.log(
  `Engine listening on Redis queue: ${env.incomingQueue}`
);

for (;;) {

  const item = await brokerClient.brPop(
    env.incomingQueue,
    0
  );

  if (!item) {
    continue;
  }

  let message: EngineRequest;

  try {

    message = JSON.parse(
      item.element
    ) as EngineRequest;

  } catch {

    console.error(
      "Skipping invalid broker message"
    );

    continue;
  }

  try {

    const data =
      handleEngineRequest(message);

    await sendResponse(
      message.responseQueue,
      {
        correlationId:
          message.correlationId,

        ok: true,

        data,
      }
    );

  } catch (error) {

    await sendResponse(
      message.responseQueue,
      {
        correlationId:
          message.correlationId,

        ok: false,

        error:
          error instanceof Error
            ? error.message
            : "engine_error",
      }
    );
  }
}