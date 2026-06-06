// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { join } from "https://deno.land/std@0.208.0/path/join.ts";
import {
  build,
  emptyDir,
  LibName,
} from "https://deno.land/x/dnt@0.39.0/mod.ts";
import { parseArgs as parseFlags } from "https://deno.land/std@0.208.0/cli/parse_args.ts";
import { generateNapiIndex } from "./generate_napi_index.ts";
import { run } from "./process.ts";

const flags = parseFlags(Deno.args);
const tests = !!flags.tests;
if (tests) console.log("including tests!");
const publish = typeof flags.publish === "string" ? flags.publish : undefined;
const dryrun = !!flags["dry-run"];
if (publish) {
  console.log(
    `publish${dryrun ? ` (dryrun)` : ""} after build! (npm=${publish})`,
  );
}
const stripLeadingV = (version: string) => version.replace(/^v/, "");
const napi = typeof flags.napi === "string"
  ? {
    packageName: "@deno/kv",
    packageVersion: stripLeadingV(flags.napi),
    artifactName: "deno-kv-napi",
  }
  : undefined;
if (napi) console.log(`napi: ${JSON.stringify(napi)}`);
if (!napi) throw new Error("Must provide --napi version");
const version = typeof Deno.args[0] === "string"
  ? stripLeadingV(Deno.args[0])
  : Deno.args[0];
if (typeof version !== "string" || !/^[a-z0-9.-]+$/.test(version)) {
  throw new Error(`Unexpected version: ${version}`);
}
console.log(`version=${version}`);

const outDir = await Deno.makeTempDir({ prefix: "userland-npm-" });
await emptyDir(outDir);

await build({
  entryPoints: [
    "./src/npm.ts",
    ...(tests ? [{ name: "./tests", path: "./src/e2e.ts" }] : []),
  ],
  outDir,
  test: false,
  shims: {
    // none!
  },
  compilerOptions: {
    // let's try to support Node 18+
    lib: [
      "ES2020",
      "DOM",
      "DOM.Iterable",
      "ESNext.Disposable",
      ...(tests ? ["ES2021.WeakRef" as LibName] : []),
    ],
    target: "ES2020",
  },
  package: {
    // package.json properties
    name: napi.packageName,
    version,
    description: "A Deno KV client library optimized for Node.js.",
    license: "MIT",
    repository: {
      type: "git",
      url: "https://github.com/denoland/denokv.git",
      directory: "npm",
    },
    bugs: {
      url: "https://github.com/denoland/denokv/issues",
    },
    homepage: "https://github.com/denoland/denokv/tree/main/npm",
    optionalDependencies: Object.fromEntries(
      [
        "win32-x64-msvc",
        "darwin-x64",
        "linux-x64-gnu",
        "linux-arm64-gnu",
        "darwin-arm64",
      ].map(
        (v) => [`${napi.packageName}-${v}`, napi.packageVersion],
      ),
    ),
  },
  async postBuild() {
    // steps to run after building and before running the tests
    await Deno.copyFile("LICENSE", join(outDir, "LICENSE"));
    await Deno.copyFile(
      "README.md",
      join(outDir, "README.md"),
    );
    const napiIndexJs = generateNapiIndex({
      napiPackageName: napi.packageName,
      napiArtifactName: napi.artifactName,
    });
    for (const subdir of ["script", "esm"]) {
      const name = "_napi_index.cjs"; // cjs to ensure 'require' works in esm mode
      console.log(`writing ${join(subdir, name)}`);
      await Deno.writeTextFile(join(outDir, subdir, name), napiIndexJs);

      console.log(`tweaking ${join(subdir, "napi_based.js")}`);
      const oldContents = await Deno.readTextFile(
        join(outDir, subdir, "napi_based.js"),
      );
      // Load the napi binding lazily-failing: platforms without a
      // binding (e.g. Deno Deploy) must still be able to import the
      // package and use the non-napi implementations. The load error
      // is kept and surfaced only when the napi path is actually used.
      const insertion = subdir === "esm"
        ? `await (async () => {
  try {
    return await import('./${name}');
  } catch (e) {
    DEFAULT_NAPI_INTERFACE_LOAD_ERROR = e;
    return undefined;
  }
})()`
        : `(() => {
  try {
    return require('./${name}');
  } catch (e) {
    DEFAULT_NAPI_INTERFACE_LOAD_ERROR = e;
    return undefined;
  }
})()`;

      const newContents = oldContents.replace(
        `const DEFAULT_NAPI_INTERFACE = undefined;`,
        `const DEFAULT_NAPI_INTERFACE = ${insertion};`,
      );
      await Deno.writeTextFile(
        join(outDir, subdir, "napi_based.js"),
        newContents,
      );
    }

    // Smoke test the built package with node on a machine without a
    // napi binding (the out dir never contains the platform packages):
    // importing must succeed, the in-memory implementation must work,
    // and the napi path must fail with the binding load error attached
    // as the cause (#99).
    console.log("smoke testing the built package");
    const smokeTest = `
(async () => {
  const assert = require('assert').strict;
  const { pathToFileURL } = require('url');
  const esm = await import(pathToFileURL(${
      JSON.stringify(join(outDir, "esm", "npm.js"))
    }));
  const cjs = require(${JSON.stringify(join(outDir, "script", "npm.js"))});
  for (const { openKv } of [esm, cjs]) {
    const kv = await openKv(undefined, { implementation: 'in-memory' });
    await kv.set(['a'], 1);
    assert.equal((await kv.get(['a'])).value, 1);
    kv.close();
    await assert.rejects(
      () => openKv('./smoke-test.sqlite'),
      (e) =>
        /No default napi interface/.test(e.message) &&
        e.cause !== undefined,
    );
  }
  console.log('smoke test ok');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
`;
    await run({ command: "node", args: ["-e", smokeTest] });
  },
});

if (publish) {
  const updatePackageJsonVersion = async (path: string, version: string) => {
    console.log(`Updating ${path} version to ${version}`);
    const packageJson = await Deno.readTextFile(path);
    const newPackageJson = packageJson.replace(
      /("version"\s*:\s*")[0-9a-z.-]+"/,
      `$1${version}"`,
    );
    if (packageJson === newPackageJson) {
      throw new Error(`Unable to replace version!`);
    }
    await Deno.writeTextFile(path, newPackageJson);
  };
  const npmPublish = async (path: string) => {
    const next = !/^[0-9]+\.[0-9]+\.[0-9]+$/.test(version);
    const out = await run({
      command: publish,
      args: [
        "publish",
        "--access",
        "public",
        ...(next ? ["--tag", "next"] : []),
        ...(dryrun ? ["--dry-run"] : []),
        path,
      ],
    });
    console.log(out);
  };

  // first, publish the native subpackages
  for (
    const { name: subdir } of (await Array.fromAsync(Deno.readDir("napi/npm")))
      .filter((v) => v.isDirectory)
  ) {
    const path = join("napi", "npm", subdir, "package.json");
    await updatePackageJsonVersion(path, version);
    await npmPublish(join("napi", "npm", subdir));
  }
  // finally, publish the root package
  await npmPublish(outDir);
}

console.log(outDir);
