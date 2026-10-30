// Builds the npm package into dist/: compiled JavaScript, type declarations
// and a package.json whose exports point at them. The repository itself
// exports the TypeScript source, so sibling projects (Kiln ERP) and the
// examples use the code as written, with no build step.
//
//   npm run build:package && cd dist && npm publish --access public
import { cpSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";

rmSync("dist", { recursive: true, force: true });
execFileSync("npx", ["tsc", "-p", "tsconfig.build.json"], { stdio: "inherit" });

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
delete pkg.scripts;
delete pkg.devDependencies;
pkg.exports = { ".": { types: "./index.d.ts", default: "./index.js" } };
pkg.types = "./index.d.ts";
pkg.files = ["*.js", "*.d.ts", "README.md", "LICENSE"];
writeFileSync("dist/package.json", JSON.stringify(pkg, null, 2) + "\n");
cpSync("README.md", "dist/README.md");
cpSync("../../LICENSE", "dist/LICENSE");
console.log(`built ${pkg.name}@${pkg.version} in dist/`);
