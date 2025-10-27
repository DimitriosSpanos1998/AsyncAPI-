import { readFile } from "node:fs/promises";
import { MongoClient } from "mongodb";
import { config } from "dotenv";
import { convert } from "@asyncapi/converter";
import YAML from "yaml";
import { randomUUID } from "node:crypto";
config();

/* ---------------------------- helpers ---------------------------- */
function ensureArray<T>(x: any): T[] {
  if (Array.isArray(x)) return x;
  if (x == null) return [];
  return [x];
}
function tagNames(tags: any) {
  return (Array.isArray(tags) ? tags : []).map((t: any) => t?.name ?? t).filter(Boolean);
}

/* --------------------- load & upgrade to v3 ---------------------- */
async function loadAndUpgrade(path: string) {
  const raw = await readFile(path, "utf8");
  const parsed = YAML.parse(raw);
  const current = parsed?.asyncapi || parsed?.version; // "2.6.0" ή "3.0.0"
  const target = "3.0.0";

  // Ήδη v3;
  if (typeof current === "string" && current.startsWith("3.")) return parsed;

  // Αλλιώς, convert → v3 (επιστρέφει YAML), μετά parse σε JSON
  const convertedYaml = convert(raw, target);
  return YAML.parse(convertedYaml);
}

/* -------- normalize operations/messages (v2 & v3 friendly) ------- */
function extractOperations(asyncapi: any, ch: any) {
  const ops: any[] = [];

  // v3 πιθανές μορφές
  if (Array.isArray(ch?.operations)) {
    for (const op of ch.operations) ops.push(op);
  } else if (ch?.operations && typeof ch.operations === "object") {
    for (const [action, op] of Object.entries(ch.operations)) {
      ops.push({ action, ...(op as any) });
    }
  }

  // v2 κλασικό στυλ πάνω στο channel
  ["publish", "subscribe"].forEach(action => {
    if (ch?.[action]) ops.push({ action, ...ch[action] });
  });

  // Κανονικοποίηση messages
  const norm = ops.map(op => {
    const action = op.action ?? op.type ?? "publish";
    const rawMsgs = Array.isArray(op.messages)
      ? op.messages
      : (op.message != null ? (Array.isArray(op.message) ? op.message : [op.message]) : []);
    const messages = rawMsgs.map((m: any) => ({
      name: m?.name,
      title: m?.title,
      summary: m?.summary,
      contentType: m?.contentType ?? asyncapi?.defaultContentType,
      schemaFormat: m?.schemaFormat,
      correlationId: m?.correlationId?.location ?? m?.correlationId,
      bindings: m?.bindings ?? {},
      examples: m?.examples ?? [],
      payloadSchema: m?.payload,
      headersSchema: m?.headers
    }));

    return {
      action,
      operationId: op?.operationId,
      summary: op?.summary,
      description: op?.description,
      tags: tagNames(op?.tags),
      security: ensureArray(op?.security).map((x: any) =>
        typeof x === "object" ? Object.keys(x)[0] : x
      ),
      bindings: op?.bindings ?? {},
      messages
    };
  });

  return norm;
}

/* ----------------------- flatten to metadata --------------------- */
function toMetadata(asyncapi: any, serviceId?: string) {
  const info = asyncapi?.info ?? {};
  const service = {
    id: serviceId ?? randomUUID(),
    title: info.title,
    version: info.version,
    defaultContentType: asyncapi.defaultContentType,
    description: info.description,
    tags: tagNames(asyncapi.tags ?? info.tags)
  };

  const servers = Object.entries(asyncapi.servers ?? {}).map(([name, s]: any) => ({
    name,
    url: s.url,
    protocol: s.protocol,
    protocolVersion: s.protocolVersion,
    description: s.description,
    security: ensureArray(s.security).map((x: any) =>
      typeof x === "object" ? Object.keys(x)[0] : x
    ),
    bindings: s.bindings ?? {},
    variables: Object.entries(s.variables ?? {}).map(([vn, v]: any) => ({
      name: vn,
      default: v?.default,
      enum: v?.enum ?? [],
      description: v?.description
    }))
  }));

  const channels = Object.entries(asyncapi.channels ?? {}).map(([name, ch]: any) => {
    const operations = extractOperations(asyncapi, ch);
    return {
      name,
      description: ch?.description,
      parameters: Object.keys(ch?.parameters ?? {}),
      bindings: ch?.bindings ?? {},
      operations
    };
  });

  const securities = Object.entries(asyncapi?.components?.securitySchemes ?? {}).map(([n, s]: any) => {
    const entry: Record<string, any> = { name: n, type: s.type };
    ["in", "scheme", "bearerFormat", "openIdConnectUrl"].forEach(k => {
      if (s[k]) entry[k] = s[k];
    });
    if (s.type === "oauth2") {
      entry.flows = Object.fromEntries(
        Object.entries(s.flows ?? {}).map(([fname, flow]: any) => [
          fname,
          {
            authorizationUrl: flow.authorizationUrl,
            tokenUrl: flow.tokenUrl,
            refreshUrl: flow.refreshUrl,
            scopes: Object.keys(flow.scopes ?? {})
          }
        ])
      );
    }
    return entry;
  });

  return { service, servers, channels, securities };
}

/* ------------------------------- main --------------------------- */
(async () => {
  const file = process.argv[2];
  const serviceId = process.argv[3];
  if (!file) {
    console.error("Usage: tsx src/convert.ts <asyncapi.(yml|yaml|json)> [serviceId]");
    process.exit(1);
  }

  const asyncapi = await loadAndUpgrade(file);   // → πάντα v3 JSON
  const metadata = toMetadata(asyncapi, serviceId);

  const client = new MongoClient(process.env.MONGO_URI!);
  await client.connect();
  const db = client.db(process.env.MONGO_DB || "aaql");

  await db.collection("originalDescriptions").insertOne({
    sourceFile: file,
    doc: asyncapi,
    insertedAt: new Date()
  });
  const res = await db.collection("metadataCollection").insertOne(metadata);

  console.log("✅ Inserted service:", metadata.service.id, "→", res.insertedId.toString());
  await client.close();
})().catch(err => {
  console.error(err);
  process.exit(1);
});
