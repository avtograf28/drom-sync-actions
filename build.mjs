// Сборка одного файла из scripts/drom-sync.mjs.
// Баннер createRequire ОБЯЗАТЕЛЕН: без него ESM-бандл падает на
// `Dynamic require of "buffer" is not supported` (cheerio использует динамический require).
import { build } from 'esbuild'

await build({
  entryPoints: ['scripts/drom-sync.mjs'],
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node22',
  outfile: 'dist/drom-sync.bundle.mjs',
  banner: {
    js: 'import { createRequire as __cr } from "module"; const require = __cr(import.meta.url);',
  },
})

console.log('built → dist/drom-sync.bundle.mjs')
