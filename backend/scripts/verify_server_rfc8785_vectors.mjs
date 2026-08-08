import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

const vectorsUrl = new URL(
  "../../docs/server/contracts/rfc8785-payload-golden-vectors-v1.json",
  import.meta.url,
);
const contract = JSON.parse(readFileSync(vectorsUrl, "utf8"));

function canonicalize(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalize(item)).join(",")}]`;
  }
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`)
    .join(",")}}`;
}

if (contract.canonicalization !== "rfc8785") {
  throw new Error("golden vector contract does not select RFC 8785");
}

for (const vector of contract.vectors) {
  const canonical = canonicalize(vector.input);
  const digest = createHash("sha256").update(canonical, "utf8").digest("hex");
  if (canonical !== vector.canonical_utf8) {
    throw new Error(`${vector.name}: canonical UTF-8 mismatch`);
  }
  if (digest !== vector.sha256) {
    throw new Error(`${vector.name}: SHA-256 mismatch`);
  }
}

console.log(`verified ${contract.vectors.length} RFC 8785 golden vectors in Node.js`);
