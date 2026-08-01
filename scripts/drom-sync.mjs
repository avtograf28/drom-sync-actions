#!/usr/bin/env node
/**
 * Drom Catalog Sync — АвтоCRM (блочный режим)
 * Обходит MAX_PAGES_PER_RUN страниц за прогон, возобновляет с last_page_reached + 1.
 * Мисматч-детекция — только при completedFull (конец каталога), через last_seen_at vs cycleStart.
 * Внешний планировщик: cron-job.org, каждые 15 минут, без временного окна
 *
 * Usage: node scripts/drom-sync.mjs [--dry-run]
 */

import { load } from 'cheerio'
import { readFileSync, existsSync } from 'fs'
import { join, dirname, resolve } from 'path'
import { fileURLToPath, pathToFileURL } from 'url'

const __dir = dirname(fileURLToPath(import.meta.url))
const ROOT    = resolve(__dir, '..')
const DRY_RUN = process.argv.includes('--dry-run')

// Откуда идёт прогон: 'actions' (GitHub Actions, отдельный публичный репо, свежий IP каждый раз)
// или 'vps-a' (резервный systemd-таймер, постоянный IP). Пишется в drom_sync_runs.run_source,
// чтобы в UI было видно, кто работает. Задаётся через env DROM_RUN_SOURCE; по умолчанию 'vps-a'.
const RUN_SOURCE = (process.env.DROM_RUN_SOURCE || 'vps-a').trim().toLowerCase()

// Уровни вывода. Публичный (всегда) — только обезличенное: страницы, счётчики, коды ответов,
// длительность. Подробности (названия, номера, цены, ссылки, размер каталога) идут через
// vlog/vwarn и печатаются ТОЛЬКО при DROM_VERBOSE (локальный запуск и резервный VPS-A, где логи
// никто посторонний не видит). Сами подробности всё равно оседают в БД: цены — price_history,
// нераспознанное — drom_unrecognized_listings, дубли — drom_number_collisions, расхождения —
// drom_mismatches, новые/изменённые позиции — inventory_items, размер каталога — drom_sync_runs.
const VERBOSE = /^(1|true|yes|on)$/i.test((process.env.DROM_VERBOSE || '').trim())
const vlog  = (...a) => { if (VERBOSE) console.log(...a) }
const vwarn = (...a) => { if (VERBOSE) console.warn(...a) }

// Адрес каталога Дрома НЕ зашит в код — берётся из окружения (env DROM_CATALOG_URL, напр.
// https://<host>/user/<seller>/). Локально можно положить в .env.local; на VPS-A он в
// /opt/drom-sync/.env, на Actions — секрет. Обязателен, как и URL базы.
function resolveCatalogUrl() {
  let u = process.env.DROM_CATALOG_URL
  if (!u) {
    const p = join(ROOT, '.env.local')
    if (existsSync(p)) {
      for (const line of readFileSync(p, 'utf8').split('\n')) {
        const m = line.match(/^\s*DROM_CATALOG_URL\s*=\s*(.+)$/)
        if (m) { u = m[1].trim().replace(/^["']|["']$/g, ''); break }
      }
    }
  }
  if (!u) throw new Error('Не задан DROM_CATALOG_URL (адрес каталога Дрома) — задайте в env или .env.local')
  return u.endsWith('/') ? u : u + '/'
}

const BASE_URL          = resolveCatalogUrl()
const UA                = 'AvtografCRM-SyncBot/1.0 (+seller Autograf28, daily catalog sync)'
const MAX_PAGES_PER_RUN = 3        // ≤3 AJAX-страницы на сессию — запас до лимита Дрома (~4
                                  // AJAX/сессия, дальше капча). Свежий разогрев каждый прогон
                                  // (см. fetchAllDromListings): непройденные страницы из чекпоинта
                                  // попадают в первые запросы свежей сессии, где хвост отдаётся
                                  // полностью. Полный круг ~82/3 ≈ 28 прогонов × 15 мин ≈ ~7 ч.
const MAX_HARD_PAGE     = 150      // аварийный потолок: если Дром зациклил пагинацию
const TIMER_INTERVAL_MIN = 30      // шаг таймера systemd (для оценки времени следующей попытки)
const REQSIG_TTL_MIN     = 120     // reqsig из кэша считаем валидным < 2 ч; иначе принудительный
                                   // разогрев (намерено ≥4 ч стабильности, берём с запасом вдвое)
// Пауза между кругами: после завершения полного круга новый прогон со страницы 1 откладывается
// на столько часов. Ставилась под баны Дрома при работе с ПОСТОЯННОГО IP. Здесь (VPS-A —
// резерв, постоянный адрес) пауза уместна, поэтому 12. В заготовке для GitHub Actions стоит 0
// (раннеры приходят с чистых IP, ограничений нет). При 0 проверка ниже не срабатывает — новый
// круг стартует сразу (быстрее находим расхождения и добираем обложки).
const CYCLE_COOLDOWN_HOURS = 12

// ── Backoff при бане Дрома (состояние в app_settings, переживает рестарты) ────
// Таймер круглосуточный (каждые 15 мин). Чтобы при 403/бане не долбиться 96 раз
// в сутки и не продлевать бан, после отказа пропускаем N ближайших прогонов:
//   1-й отказ подряд  → пропустить 2  (следующая попытка ~через 45 мин)
//   2-й подряд        → пропустить 8  (~2 ч)
//   3-й и далее       → пропустить 24 (~6 ч)
// Успешный контакт с Дромом обнуляет счётчик.
function backoffSkipsFor(failCount) {
  if (failCount <= 1) return 2
  if (failCount === 2) return 8
  return 24
}

// Бросается, когда Дром вернул 403 (бан IP) — сигнал для backoff.
class DromBanError extends Error {
  constructor(status) {
    super(`Drom ban: HTTP ${status}`)
    this.name = 'DromBanError'
    this.status = status
  }
}

// ── Env ───────────────────────────────────────────────────────────────────────

function loadConfig() {
  const fromEnv = {
    url: process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL,
    key: process.env.SUPABASE_SERVICE_ROLE_KEY,
  }
  if (fromEnv.url && fromEnv.key) return fromEnv

  const envPath = join(ROOT, '.env.local')
  if (!existsSync(envPath)) {
    throw new Error(
      'Конфиг не найден: задайте SUPABASE_URL и SUPABASE_SERVICE_ROLE_KEY в env, ' +
      'или создайте .env.local для локального запуска'
    )
  }
  const env = {}
  for (const line of readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^([A-Z0-9_]+)=(.+)$/)
    if (m) env[m[1]] = m[2].trim().replace(/^["']|["']$/g, '')
  }
  const url = env.NEXT_PUBLIC_SUPABASE_URL
  const key = env.SUPABASE_SERVICE_ROLE_KEY || env.SERVICE_ROLE_KEY
  if (!url || !key) {
    throw new Error(
      'В .env.local отсутствуют NEXT_PUBLIC_SUPABASE_URL или SUPABASE_SERVICE_ROLE_KEY'
    )
  }
  return { url, key }
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms))
}

function randomDelay() {
  // 6–12 сек между страницами
  return sleep(6000 + Math.random() * 6000)
}

// ── Cookie jar ────────────────────────────────────────────────────────────────

function extractCookies(res) {
  const jar = {}
  const setCookies = typeof res.headers.getSetCookie === 'function'
    ? res.headers.getSetCookie()
    : []
  for (const header of setCookies) {
    const eqIdx   = header.indexOf('=')
    const semiIdx = header.indexOf(';')
    if (eqIdx === -1) continue
    const name  = header.slice(0, eqIdx).trim()
    const value = (semiIdx === -1
      ? header.slice(eqIdx + 1)
      : header.slice(eqIdx + 1, semiIdx)
    ).trim()
    if (name) jar[name] = value
  }
  return jar
}

function buildCookieHeader(jar) {
  return Object.entries(jar).map(([k, v]) => `${k}=${v}`).join('; ')
}

function mergeCookies(existing, res) {
  return { ...existing, ...extractCookies(res) }
}

// ── Drom fetch: warmup + AJAX pages ──────────────────────────────────────────

async function warmupSession() {
  vlog(`  Разогрев сессии: GET ${BASE_URL}`)
  const res = await fetch(BASE_URL, {
    headers: {
      'User-Agent':      UA,
      'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'ru-RU,ru;q=0.9',
    },
    signal: AbortSignal.timeout(30_000),
  })
  if (res.status === 403) throw new DromBanError(403)
  if (!res.ok) {
    console.warn(`  ⚠️  Разогрев вернул HTTP ${res.status}`)
  }

  const jar  = res.ok ? extractCookies(res) : {}
  const html = res.ok
    ? new TextDecoder('windows-1251').decode(await res.arrayBuffer())
    : ''

  let reqsig = null
  const bodyMatch = html.match(/data-reqsig="([a-f0-9]+)"/i)
  if (bodyMatch) reqsig = bodyMatch[1]

  if (!reqsig) {
    const scriptMatch = html.match(/<script[^>]*id="ctr-viewdir-event"[^>]*>([\s\S]*?)<\/script>/i)
    if (scriptMatch) {
      try {
        const json = JSON.parse(scriptMatch[1].trim())
        if (json.reqsig) reqsig = json.reqsig
      } catch { /* ignore */ }
    }
  }

  if (!reqsig) {
    throw new Error(
      '❌ Не удалось извлечь reqsig из HTML разогрева — структура страницы могла измениться'
    )
  }

  vlog(`  Разогрев успешен, куков: ${Object.keys(jar).length}, reqsig: ${reqsig}`)
  return { cookieJar: jar, reqsig }
}

function charsetFromContentType(contentTypeHeader) {
  if (!contentTypeHeader) return 'windows-1251'
  const m = contentTypeHeader.match(/charset=([^\s;]+)/i)
  return m ? m[1].toLowerCase() : 'windows-1251'
}

let ajaxContentTypeLogged = false

async function fetchAjaxPage(pageNum, cookieJar, reqsig) {
  const url =
    `${BASE_URL}?_lightweight=1&ajax=1&async=1&city=0&fieldSetHash=&page=${pageNum}&status=actual`

  const headers = {
    'User-Agent':        UA,
    'Accept':            'application/json, text/javascript, */*; q=0.01',
    'Accept-Language':   'ru-RU,ru;q=0.9',
    'Referer':           BASE_URL,
    'X-Requested-With':  'XMLHttpRequest',
    'x-referrer-reqsig': reqsig,
  }

  const cookieStr = buildCookieHeader(cookieJar)
  if (cookieStr) headers['Cookie'] = cookieStr

  const res = await fetch(url, {
    headers,
    signal: AbortSignal.timeout(30_000),
  })

  if (!res.ok) throw new Error(`HTTP ${res.status} для страницы ${pageNum}`)

  const updatedJar = mergeCookies(cookieJar, res)

  const ct      = res.headers.get('content-type') ?? ''
  const charset = charsetFromContentType(ct)
  if (!ajaxContentTypeLogged) {
    console.log(`  Content-Type AJAX: "${ct}" → декодируем как ${charset}`)
    ajaxContentTypeLogged = true
  }

  const buf  = await res.arrayBuffer()
  const text = new TextDecoder(charset).decode(buf)
  return { text, cookieJar: updatedJar }
}

// ── Anti-bot interstitial detection ──────────────────────────────────────────

function isInterstitial(text) {
  const t = text.trimStart()
  if (!t.startsWith('{')) return true
  if (t.includes('Вы не робот') || t.includes('подозрительный трафик')) return true
  return false
}

// ── Supabase REST helpers ─────────────────────────────────────────────────────

function supaHeaders(key) {
  return {
    apikey:         key,
    Authorization:  `Bearer ${key}`,
    'Content-Type': 'application/json',
  }
}

async function supaGet(url, key, path, params = '') {
  const fullUrl = `${url}/rest/v1/${path}${params ? '?' + params : ''}`
  const res = await fetch(fullUrl, { headers: supaHeaders(key) })
  if (!res.ok) {
    const txt = await res.text()
    throw new Error(
      `GET ${path} failed — HTTP ${res.status}\n` +
      `  URL: ${fullUrl.replace(key, '[REDACTED]')}\n` +
      `  Body: ${txt.slice(0, 500)}`
    )
  }
  const json = await res.json()
  if (!Array.isArray(json)) {
    throw new Error(
      `GET ${path} — ожидался массив, получено: ${JSON.stringify(json).slice(0, 200)}`
    )
  }
  return json
}

async function supaPost(url, key, path, body, prefer = '', allowedStatuses = []) {
  const headers = { ...supaHeaders(key) }
  if (prefer) headers['Prefer'] = prefer
  const res = await fetch(`${url}/rest/v1/${path}`, {
    method:  'POST',
    headers,
    body:    JSON.stringify(body),
  })
  if (!res.ok && !allowedStatuses.includes(res.status)) {
    const txt = await res.text()
    throw new Error(`POST ${path} failed (${res.status}): ${txt}`)
  }
  return res
}

async function supaPatch(url, key, path, params, body) {
  const res = await fetch(`${url}/rest/v1/${path}?${params}`, {
    method:  'PATCH',
    headers: { ...supaHeaders(key), Prefer: 'return=minimal' },
    body:    JSON.stringify(body),
  })
  if (!res.ok) {
    const txt = await res.text()
    throw new Error(`PATCH ${path} failed (${res.status}): ${txt}`)
  }
}

async function supaDelete(url, key, path, params) {
  const res = await fetch(`${url}/rest/v1/${path}?${params}`, {
    method:  'DELETE',
    headers: { ...supaHeaders(key), Prefer: 'return=minimal' },
  })
  if (!res.ok) {
    const txt = await res.text()
    throw new Error(`DELETE ${path} failed (${res.status}): ${txt}`)
  }
}

// ── Storage REST (тот же хост и service-ключ, что PostgREST) ───────────────────

async function storageUpload(url, key, bucket, path, bytes, contentType) {
  const res = await fetch(`${url}/storage/v1/object/${bucket}/${path}`, {
    method:  'POST',
    headers: {
      apikey:         key,
      Authorization:  `Bearer ${key}`,
      'Content-Type': contentType,
      'x-upsert':     'true',   // повторная заливка того же пути перезаписывает (идемпотентно при ретрае)
    },
    body: bytes,
  })
  if (!res.ok) {
    const txt = await res.text()
    throw new Error(`Storage upload ${bucket}/${path} failed (${res.status}): ${txt.slice(0, 300)}`)
  }
}

// Возвращает true при успешном удалении, false — иначе (НЕ бросает: вызывающий решает,
// удалять ли строку). remove() без успеха → строку в БД не трогаем, иначе будет висеть
// запись на несуществующий файл.
async function storageRemove(url, key, bucket, path) {
  const res = await fetch(`${url}/storage/v1/object/${bucket}/${path}`, {
    method:  'DELETE',
    headers: { apikey: key, Authorization: `Bearer ${key}` },
  })
  return res.ok
}

// ── Cover photos (обложка объявления с Дрома) ─────────────────────────────────

const COVER_BUCKET      = 'inventory-photos'
// Отрицательный sort_order → обложка первая в списке фото (UI сортирует по возрастанию),
// а фото специалистов (всегда >= 0) сохраняют свой относительный порядок и не трогаются.
const COVER_SORT_ORDER  = -1
// Не тянем обложки для терминальных статусов: ретеншн их чистит → был бы пинг-понг.
const COVER_SKIP_STATUS = new Set(['sold', 'written_off', 'returned'])
// Пауза между скачиваниями обложек — размазать всплеск запросов к Дрому (static.baza в том же
// /24, что и каталог; ~95 картинок за прогон переполняли лимит и ловили 403-бан на разогреве).
const COVER_DOWNLOAD_DELAY_MS = 400
// Выключатель скачивания обложек. При false обложки НЕ качаются вообще (на Дром за картинками
// не ходим), но drom_photo_id в inventory_items пишется как обычно — это запись в нашу БД, на
// Дром не ходит. Включено: баны были из-за IP, а GitHub Actions каждый раз с нового адреса.
// Если резервный VPS-A (постоянный IP) снова начнёт ловить 403 — поставить false и пересобрать.
const COVERS_ENABLED = true
// Потолок скачиваний обложек за ОДИН ПРОГОН (не за страницу!). Бюджет общий на прогон — объект
// coverBudget создаётся в onPageComplete-скоупе и передаётся в updateExistingItems, поэтому не
// сбрасывается между страницами блока. Раньше счётчик жил внутри updateExistingItems (вызов на
// каждую страницу) → 50/страницу × 2 страницы = ~100 за прогон, потолок не держал.
const MAX_COVERS_PER_RUN = 50

// Скачивает полноразмерную обложку {PHOTO_ID}_full (webp 1280×960). Бросает при не-2xx
// или если пришла не картинка (антибот-заглушка) — чтобы не сохранить HTML под .webp.
async function fetchDromImageFull(photoId) {
  const res = await fetch(`https://static.baza.drom.ru/v/${photoId}_full`, {
    headers: { 'User-Agent': UA, 'Referer': BASE_URL },
    signal:  AbortSignal.timeout(30_000),
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  const ct = res.headers.get('content-type') ?? ''
  if (!ct.startsWith('image/')) throw new Error(`не картинка (content-type: ${ct || 'нет'})`)
  return Buffer.from(await res.arrayBuffer())
}

// Удаляет зависшие обложки другого drom_photo_id: сначала файл, строку — только если файл
// удалён. Не удалось удалить файл → строку оставляем и логируем (никогда не трогаем чужие
// фото — фильтр по source='drom_sync' сделан на уровне выборки rows у вызывающего).
async function cleanStaleCovers(supaUrl, key, itemId, staleRows) {
  for (const old of staleRows) {
    if (old.storage_path) {
      const removed = await storageRemove(supaUrl, key, COVER_BUCKET, old.storage_path)
      if (!removed) {
        console.warn(`  ⚠️  Обложка ${itemId}: файл ${old.storage_path} не удалён из Storage — строку оставляю`)
        continue
      }
    }
    await supaDelete(supaUrl, key, 'inventory_photos', `id=eq.${old.id}`)
  }
}

// Качает {photoId}_full, заливает в Storage и вставляет строку обложки (sort_order=-1).
async function downloadUploadInsertCover(supaUrl, key, itemId, photoId, newId) {
  const path   = `${itemId}/drom-${photoId}.webp`
  const srcUrl = `https://static.baza.drom.ru/v/${photoId}_full`
  const bytes  = await fetchDromImageFull(photoId)
  await storageUpload(supaUrl, key, COVER_BUCKET, path, bytes, 'image/webp')
  await supaPost(supaUrl, key, 'inventory_photos', {
    inventory_item_id: itemId,
    storage_path:      path,
    sort_order:        COVER_SORT_ORDER,
    source:            'drom_sync',
    source_url:        srcUrl,
    drom_photo_id:     newId,
  }, 'return=minimal')
}

// Синхронизирует обложку одной позиции. Возвращает 'loaded' | 'updated' | 'skipped_own' | 'skipped'.
//   savedPhotoId — inventory_items.drom_photo_id (последний увиденный id обложки) ДО этого прогона.
// Бросает при ошибке скачивания/заливки/вставки — вызывающий считает это ошибкой и идёт дальше.
async function syncCoverForItem(supaUrl, key, itemId, status, photoId, savedPhotoId) {
  if (!photoId) return 'skipped'
  if (COVER_SKIP_STATUS.has(status)) return 'skipped'

  const rows = await supaGet(
    supaUrl, key, 'inventory_photos',
    `select=id,source,drom_photo_id,storage_path,purged_at&inventory_item_id=eq.${itemId}`
  )
  // Ретеншн удалил обложку осознанно (purged_at заполнен) → не воскрешаем.
  if (rows.some(r => r.source === 'drom_sync' && r.purged_at !== null)) return 'skipped'

  const hasOwn      = rows.some(r => r.source !== 'drom_sync')
  const dromRows    = rows.filter(r => r.source === 'drom_sync')
  const newId       = Number(photoId)
  const staleActive = dromRows.filter(r => Number(r.drom_photo_id) !== newId)

  if (hasOwn) {
    // Есть свои фото (человек снял деталь и то же фото опубликовал на Дроме). Обложку тянем
    // ТОЛЬКО когда Дром сменил фото относительно сохранённого id:
    //   savedPhotoId пусто        → baseline: id только проставляется (вызывающим), не качаем;
    //   savedPhotoId == newId      → фото не менялось, не качаем;
    //   обложка синка с newId есть → не дублируем.
    const saved   = savedPhotoId == null ? null : Number(savedPhotoId)
    const haveNew = dromRows.some(r => Number(r.drom_photo_id) === newId)
    if (saved === null || saved === newId || haveNew) return 'skipped_own'

    // Фото на Дроме заменили → тянем новую обложку рядом со своими фото. Свои не трогаем;
    // прежнюю обложку синка (другой id) снимаем после успешной заливки новой.
    await downloadUploadInsertCover(supaUrl, key, itemId, photoId, newId)
    await cleanStaleCovers(supaUrl, key, itemId, staleActive)
    return 'updated'
  }

  // Нет своих фото — прежняя логика: актуальная обложка уже есть → ничего не качаем.
  const activeSame = dromRows.find(r => Number(r.drom_photo_id) === newId) ?? null
  if (activeSame) {
    await cleanStaleCovers(supaUrl, key, itemId, staleActive)
    return 'skipped'
  }

  // Обложки нет либо id сменился → качаем и подчищаем прежнюю после успешной заливки.
  await downloadUploadInsertCover(supaUrl, key, itemId, photoId, newId)
  const wasReplacement = staleActive.length > 0
  await cleanStaleCovers(supaUrl, key, itemId, staleActive)
  return wasReplacement ? 'updated' : 'loaded'
}

// ── Cycle helpers (app_settings key-value) ────────────────────────────────────

async function getCycleStart(supaUrl, key) {
  const rows = await supaGet(supaUrl, key, 'app_settings', 'select=value&key=eq.drom_cycle_started_at')
  return rows[0]?.value ?? null
}

async function getCycleCompleted(supaUrl, key) {
  const rows = await supaGet(supaUrl, key, 'app_settings', 'select=value&key=eq.drom_cycle_completed_at')
  return rows[0]?.value ?? null
}

async function setCycleCompleted(supaUrl, key, ts) {
  await supaPost(
    supaUrl, key,
    'app_settings',
    { key: 'drom_cycle_completed_at', value: ts },
    'resolution=merge-duplicates,return=minimal'
  )
}

async function setCycleStart(supaUrl, key, ts) {
  await supaPost(
    supaUrl, key,
    'app_settings',
    { key: 'drom_cycle_started_at', value: ts },
    'resolution=merge-duplicates,return=minimal'
  )
}

// ── Backoff state (app_settings key-value) ────────────────────────────────────

async function getBackoff(supaUrl, key) {
  const rows = await supaGet(
    supaUrl, key, 'app_settings',
    'select=key,value&key=in.(drom_backoff_fail_count,drom_backoff_skip_remaining,drom_backoff_last_403_at)'
  )
  const m = {}
  for (const r of rows) m[r.key] = r.value
  const num = (v) => { const n = parseInt(v ?? '0', 10); return Number.isFinite(n) ? n : 0 }
  return {
    failCount:     num(m.drom_backoff_fail_count),
    skipRemaining: num(m.drom_backoff_skip_remaining),
    last403At:     m.drom_backoff_last_403_at ?? null,
  }
}

async function setSetting(supaUrl, key, k, v) {
  await supaPost(
    supaUrl, key, 'app_settings',
    { key: k, value: String(v) },
    'resolution=merge-duplicates,return=minimal'
  )
}

// ── reqsig cache (app_settings: drom_reqsig, drom_reqsig_at) ──────────────────
// Сессия Дрома держится на reqsig + IP (кук нет). reqsig стабилен ≥4 ч, поэтому
// кэшируем его и пропускаем разогрев, пока моложе REQSIG_TTL_MIN. Это убирает
// GET страницы юзера — тот самый запрос, что первым ловит бан.

async function getCachedReqsig(supaUrl, key) {
  const rows = await supaGet(
    supaUrl, key, 'app_settings',
    'select=key,value&key=in.(drom_reqsig,drom_reqsig_at)'
  )
  const m = {}
  for (const r of rows) m[r.key] = r.value
  if (!m.drom_reqsig || !m.drom_reqsig_at) return null
  const atMs = Date.parse(m.drom_reqsig_at)
  if (!Number.isFinite(atMs)) return null
  const ageMin = Math.round((Date.now() - atMs) / 60_000)
  return { reqsig: m.drom_reqsig, ageMin }
}

async function saveReqsig(supaUrl, key, reqsig) {
  await setSetting(supaUrl, key, 'drom_reqsig',    reqsig)
  await setSetting(supaUrl, key, 'drom_reqsig_at', new Date().toISOString())
}

// Оценка времени следующей реальной попытки: следующий тик через TIMER_INTERVAL_MIN,
// из них skipRemaining будут пропущены → попытка через (skipRemaining+1)*шаг.
function nextAttemptEtaUtc(skipRemaining) {
  const etaMin = (skipRemaining + 1) * TIMER_INTERVAL_MIN
  return new Date(Date.now() + etaMin * 60_000).toISOString().slice(11, 16)
}

// ── Per-page cursor (survives crash before Phase 8) ───────────────────────────

async function getLastPageReached(supaUrl, key) {
  const rows = await supaGet(supaUrl, key, 'app_settings', 'select=value&key=eq.drom_last_page_reached')
  const val = rows[0]?.value ?? null
  if (val === null || val === 'null') return null
  const n = parseInt(val, 10)
  return Number.isFinite(n) ? n : null
}

async function setLastPageReached(supaUrl, key, page) {
  await supaPost(
    supaUrl, key,
    'app_settings',
    { key: 'drom_last_page_reached', value: page === null ? 'null' : String(page) },
    'resolution=merge-duplicates,return=minimal'
  )
}

// ── Resume helper ─────────────────────────────────────────────────────────────

async function checkLastRun(supaUrl, key) {
  // Primary cursor: app_settings (written per-page, survives crash before Phase 8)
  const appPage = await getLastPageReached(supaUrl, key)
  if (appPage !== null) return appPage + 1

  // Fallback: drom_sync_runs (used on first-ever run or if app_settings key not yet set)
  const rows = await supaGet(
    supaUrl, key,
    'drom_sync_runs',
    'select=last_page_reached&order=run_at.desc&limit=1'
  )
  const lastPage = rows[0]?.last_page_reached ?? null
  if (lastPage == null) return 1
  return lastPage + 1
}

// ── Drom HTML parser ──────────────────────────────────────────────────────────

// Matches internal numbers in parens at the end of a listing title.
const NUM_RE      = /\(([A-Za-zА-Яа-я0-9]+(?:\/\d+)?)\)?\s*$/
// Fallback: bare numeric token (optionally /N) preceded by whitespace at end of title.
const BARE_NUM_RE = /\s(\d+(?:\/\d+)?)\s*$/

function extractInternalNumber(title) {
  const m = NUM_RE.exec(title)
  if (m) return m[1]
  const m2 = BARE_NUM_RE.exec(title)
  if (m2) return m2[1]
  return null
}

function parseFeed(feedHtml) {
  const $ = load(feedHtml)
  const items        = []
  const unrecognized = []

  $('.bull-item').each((_, el) => {
    const $el   = $(el)
    const $link = $el.find('a.bull-item__self-link').first()
    const title = $link.text().trim()
    const href  = $link.attr('href') ?? ''

    const priceRaw  = $el.find('.price-block__price[data-role="price"]').first().text()
    const dromPrice = priceRaw ? parseInt(priceRaw.replace(/\D/g, ''), 10) || null : null
    const dateRaw   = $el.find('.bull-item-info__value .date').first().text().trim() || null

    // Обложка объявления: <img class="bull-image-preloader" src=".../v/{PHOTO_ID}_block">.
    // PHOTO_ID — числовой (мс-таймстамп загрузки фото), меняется при замене обложки.
    const imgSrc  = $el.find('img.bull-image-preloader').first().attr('src') ?? ''
    const photoM  = imgSrc.match(/\/v\/(\d+)_/)
    const photoId = photoM ? photoM[1] : null

    const internalNumber = extractInternalNumber(title)
    if (!internalNumber) {
      unrecognized.push({ title, url: href })
      return
    }

    const name = title.replace(NUM_RE, '').replace(BARE_NUM_RE, '').trim()
    items.push({ internalNumber, name, dromPrice, dateRaw, url: href, photoId })
  })

  return { items, unrecognized }
}

// ── Side parser (ported from src/lib/parseItemTitle.ts) ──────────────────────

const FRONT_RE = /^перед\p{L}*$/iu
const BACK_RE  = /^задн\p{L}*$/iu
const LEFT_RE  = /^лев\p{L}*$/iu
const RIGHT_RE = /^прав\p{L}*$/iu
const UPPER_RE = /^верх\p{L}*$/iu
const LOWER_RE = /^нижн\p{L}*$/iu

function isPositionWord(w) {
  return FRONT_RE.test(w) || BACK_RE.test(w) ||
         LEFT_RE.test(w)  || RIGHT_RE.test(w) ||
         UPPER_RE.test(w) || LOWER_RE.test(w)
}

// Returns side string (e.g. 'F.L', 'R', 'F.R ↑ Верх') or null if undetermined.
// Scans all tokens — Latin OEM codes won't match Cyrillic regexes, so no false positives.
//
// Пара (в названии названы И левая, И правая) сторону НЕ конкретизирует — ориентируемся
// только на перёд/зад (F или R). Так продаётся, напр., «стойка передняя левая правая» —
// это перёд (F), а не F.L. Одиночная сторона сохраняется (F.L/F.R/…). Слэши приводим к
// пробелам, чтобы «левый/правый» и «левая /правая» тоже дали обе стороны (иначе токен со
// слэшем не матчится LEFT_RE/RIGHT_RE и пара ошибочно схлопывается в одну сторону).
function parseItemSide(fullName) {
  const words = fullName.replace(/\//g, ' ').trim().split(/\s+/).filter(Boolean)

  const firstPos = words.findIndex(isPositionWord)
  if (firstPos === -1) return null

  const posWords = words.slice(firstPos)

  const hasFront = posWords.some(w => FRONT_RE.test(w))
  const hasBack  = posWords.some(w => BACK_RE.test(w))
  let   hasLeft  = posWords.some(w => LEFT_RE.test(w))
  let   hasRight = posWords.some(w => RIGHT_RE.test(w))
  const hasUpper = posWords.some(w => UPPER_RE.test(w))
  const hasLower = posWords.some(w => LOWER_RE.test(w))

  // Обе стороны названы (пара) → L/R не уточняем, остаётся только перёд/зад.
  if (hasLeft && hasRight) { hasLeft = false; hasRight = false }

  let base = null
  if      (hasFront && hasLeft)  base = 'F.L'
  else if (hasFront && hasRight) base = 'F.R'
  else if (hasBack  && hasLeft)  base = 'R.L'
  else if (hasBack  && hasRight) base = 'R.R'
  else if (hasFront)             base = 'F'
  else if (hasBack)              base = 'R'
  // standalone left/right without front/back → ambiguous, leave null

  if      (base && hasUpper) base = `${base} ↑ Верх`
  else if (base && hasLower) base = `${base} ↓ Низ`

  return base
}

// ── Phase 1: Fetch Drom listings block (resume + limit) ───────────────────────

async function fetchAllDromListings(supaUrl, key, startPage, onPageComplete = null) {
  console.log(
    `  Старт с страницы ${startPage}` +
    ` (блок: страницы ${startPage}–${startPage + MAX_PAGES_PER_RUN - 1})`
  )

  // Свежая сессия на КАЖДЫЙ прогон: reqsig между прогонами НЕ переиспользуем. У Дрома лимит
  // ~4 AJAX-запроса на сессию; переиспользование reqsig копило запросы одной сессии и глушило
  // хвост (пустой feed → ложное завершение круга). Разогрев каждый раз кладёт непройденные
  // страницы (из чекпоинта) в ПЕРВЫЕ запросы свежей сессии, где хвост отдаётся полностью.
  let cookieJar = {}
  let reqsig
  let freshWarmup = true    // всегда свежий разогрев (флаг оставлен для ветки refresh-on-403 ниже)

  try {
    const w = await warmupSession()
    cookieJar = w.cookieJar
    reqsig    = w.reqsig
    if (!DRY_RUN) await saveReqsig(supaUrl, key, reqsig)  // пишем только для наблюдаемости — не читается
    console.log('  Свежий разогрев — reqsig получен')
  } catch (e) {
    if (e instanceof DromBanError) {
      console.warn('  ⚠️  403 на разогреве — Дром не пустил вообще, включаю backoff')
      return {
        allItems: [], allUnrecognized: [], pagesScanned: 0, completedFull: false,
        lastPageReached: null, startPage, blockCollisions: [], warmupBanned: true, itemsCount: null,
        completedAtPage: null,
      }
    }
    throw e
  }
  await sleep(2000 + Math.random() * 2000)

  const allItems         = []
  const seenNumbers      = new Map()   // internalNumber → first item seen this block
  const blockCollisions  = []
  const seenUnrecognized = new Set()
  const allUnrecognized  = []
  let   pagesScanned     = 0
  let   completedFull    = false
  let   completedAtPage  = null   // номер страницы, на которой круг реально завершился (для guard)
  let   lastPageReached  = null
  let   itemsCount       = null

  for (let pg = startPage; ; pg++) {
    const pageLabel = (VERBOSE && itemsCount != null)
      ? `Страница ${pg} / ~${Math.ceil(itemsCount / 50)}`
      : `Страница ${pg}`
    console.log(`  ${pageLabel}`)

    let rawText    = null
    let stopped    = false   // завершить блок штатно (лимит частоты / ошибка) — не бан
    let midRunBan  = false   // настоящий бан, обнаруженный при refresh-разогреве

    // Получение страницы. Если reqsig был из кэша и Дром отдал 403/заглушку — считаем
    // reqsig протухшим, обновляем разогревом и повторяем страницу ОДИН раз. Разогрев тут
    // же служит пробой на бан: 403 на разогреве = настоящий бан → backoff. Если reqsig
    // уже свежий (разогрев в этом прогоне), 403 = лимит частоты, блок завершается штатно.
    for (;;) {
      let fetchErr = null
      try {
        const result = await fetchAjaxPage(pg, cookieJar, reqsig)
        rawText   = result.text
        cookieJar = result.cookieJar
      } catch (e) {
        fetchErr = e
      }

      if (fetchErr != null && !fetchErr.message.startsWith('HTTP 403')) {
        console.error(`  ❌ Ошибка страницы ${pg}: ${fetchErr.message}`)
        stopped = true
        break
      }

      const is403   = fetchErr != null && fetchErr.message.startsWith('HTTP 403')
      const softBad = is403 || (fetchErr == null && isInterstitial(rawText))

      if (softBad) {
        if (!freshWarmup) {
          // reqsig был из кэша → возможно протух. Обновляем разогревом и пробуем снова.
          console.warn(`  ⚠️  ${is403 ? '403' : 'антибот-заглушка'} на стр. ${pg} с кэшированным reqsig — обновляю разогревом`)
          try {
            const w = await warmupSession()
            cookieJar   = w.cookieJar
            reqsig      = w.reqsig
            freshWarmup = true
            if (!DRY_RUN) await saveReqsig(supaUrl, key, reqsig)
            console.log(`  reqsig обновлён — повтор страницы ${pg}`)
            continue   // повтор той же страницы со свежим reqsig
          } catch (we) {
            if (we instanceof DromBanError) {
              console.warn('  ⚠️  403 на разогреве при обновлении reqsig — настоящий бан, backoff')
              midRunBan = true
              break
            }
            throw we
          }
        }
        // reqsig уже свежий → это лимит частоты, а не бан. Завершаем блок штатно.
        console.warn(`  ⚠️  ${is403 ? '403' : 'антибот-заглушка'} на стр. ${pg} — лимит частоты, завершаю блок штатно (не бан)`)
        stopped = true
        break
      }

      break   // успех: валидный JSON получен
    }

    if (midRunBan) {
      // Настоящий бан посреди блока: чекпоинт по успешным страницам уже сохранён,
      // backoff взводится в main. Дальше блок не продолжаем.
      return {
        allItems, allUnrecognized, pagesScanned, completedFull: false,
        lastPageReached, startPage, blockCollisions, warmupBanned: true, itemsCount,
        completedAtPage: null,
      }
    }
    if (stopped) break

    let data
    try {
      const json = JSON.parse(rawText)
      data = json.data ?? json
    } catch (e) {
      console.error(`  ❌ Ошибка JSON страницы ${pg}: ${e.message}`)
      break
    }

    if (itemsCount == null && data.itemsCount != null) {
      itemsCount = Number(data.itemsCount)
      vlog(`  Всего объявлений по данным Дрома: ${itemsCount}`)
    }

    const feedHtml = data.feed ?? ''
    const { items, unrecognized } = parseFeed(feedHtml)

    if (items.length === 0 && unrecognized.length === 0) {
      const expectedLast = itemsCount != null ? Math.ceil(itemsCount / 50) : null
      if (expectedLast != null && pg >= expectedLast - 1) {
        // Пусто у ОЖИДАЕМОГО конца каталога — настоящий конец.
        console.log(`  Страница ${pg}: пусто у ожидаемого конца (~${expectedLast}) — конец каталога`)
        completedFull   = true
        completedAtPage = pg
        lastPageReached = null  // next run starts from page 1
      } else {
        // Пустой feed ДАЛЕКО от ожидаемого конца = антибот/старвейшн хвоста, НЕ конец каталога.
        // Мягкий сбой: круг НЕ завершаем (completedFull остаётся false), чекпоинт — на последней
        // успешной странице; следующий прогон (свежая сессия) добирает эти страницы.
        console.warn(`  ⚠️  Страница ${pg}: пустой feed далеко от конца (ожидалось ~${expectedLast ?? '?'}) — мягкий сбой (антибот/старвейшн), круг НЕ завершаю`)
      }
      break
    }

    let newCount = 0
    const pageItems = []
    for (const item of items) {
      if (!seenNumbers.has(item.internalNumber)) {
        seenNumbers.set(item.internalNumber, item)
        allItems.push(item)
        pageItems.push(item)
        newCount++
      } else if (seenNumbers.get(item.internalNumber).name !== item.name) {
        const first = seenNumbers.get(item.internalNumber)
        const sideA = parseItemSide(first.name)
        const sideB = parseItemSide(item.name)
        vlog(
          `\n⚠️  КОЛЛИЗИЯ НОМЕРА: №${item.internalNumber}` +
          `\n   A: "${first.name}" (сторона: ${sideA ?? 'не распознана'})` +
          `\n      https://baza.drom.ru${first.url || ''}` +
          `\n   B: "${item.name}" (сторона: ${sideB ?? 'не распознана'})` +
          `\n      https://baza.drom.ru${item.url || ''}`
        )
        blockCollisions.push({
          internal_number: item.internalNumber,
          url_a:  first.url ?? '',
          name_a: first.name,
          side_a: sideA,
          url_b:  item.url ?? '',
          name_b: item.name,
          side_b: sideB,
        })
      }
      // else: одинаковое имя → артефакт пагинации, тихий drop
    }
    const pageUnrecognized = []
    for (const u of unrecognized) {
      if (!seenUnrecognized.has(u.url)) {
        seenUnrecognized.add(u.url)
        allUnrecognized.push(u)
        pageUnrecognized.push(u)
      }
    }

    // Per-page callback: import + update + checkpoint.
    // Throws on error → page not counted, block aborts, cursor stays at last good page.
    if (onPageComplete) {
      await onPageComplete(pg, pageItems, pageUnrecognized)
    }

    // Count page as processed only after callback succeeds
    pagesScanned++
    lastPageReached = pg

    console.log(
      `  Страница ${pg}: ${items.length} записей (новых: ${newCount}), ` +
      `нераспознано ${unrecognized.length}. Собрано за блок: ${allItems.length}`
    )

    // Natural end of catalog: current page number >= total pages reported by Drom.
    // Uses pg (current page number) not allItems.length, because allItems only
    // accumulates within this block — it can never reach the global itemsCount.
    if (itemsCount != null && pg >= Math.ceil(itemsCount / 50)) {
      console.log(`  Достигнут конец каталога (стр. ${pg}) — готово`)
      completedFull   = true
      completedAtPage = pg
      lastPageReached = null
      break
    }

    // Hard safety cap: Drom may loop pagination past the real end, returning
    // non-empty pages indefinitely. If we hit this, force completedFull=true
    // and log a clear warning so it's not confused with a normal cycle end.
    if (pg >= MAX_HARD_PAGE) {
      console.warn(
        `\n⚠️  АВАРИЙНЫЙ ПОТОЛОК: достигнута страница ${pg} (MAX_HARD_PAGE=${MAX_HARD_PAGE}).` +
        `\n   Дром вероятно зациклил пагинацию — принудительно завершаем цикл.` +
        `\n   Проверьте структуру каталога baza.drom.ru вручную.`
      )
      completedFull   = true
      completedAtPage = pg
      lastPageReached = null
      break
    }

    // Block limit: stop after MAX_PAGES_PER_RUN successfully scanned pages
    if (pagesScanned >= MAX_PAGES_PER_RUN) {
      console.log(`  Блок из ${MAX_PAGES_PER_RUN} стр. завершён (страница ${pg}) — до следующего запуска`)
      break
    }

    await randomDelay()
  }

  // If we errored before processing any page of this block, preserve position for retry
  if (pagesScanned === 0 && startPage > 1) {
    lastPageReached = startPage - 1
  }

  const status = completedFull ? '✅ Полный круг завершён' : `⏸  Блок ${startPage}–${lastPageReached ?? startPage} выполнен`
  console.log(`\n📋 Статус: ${status}`)
  // warmupBanned=false: разогрев прошёл. Даже если внутри блока был 403/заглушка
  // (лимит частоты) — это НЕ бан, backoff не включаем, чекпоинт сохранён.
  return { allItems, allUnrecognized, pagesScanned, completedFull, lastPageReached, startPage, blockCollisions, warmupBanned: false, itemsCount, completedAtPage }
}

// ── Phase 2: Load CRM snapshot ────────────────────────────────────────────────

async function loadCrmSnapshot(supaUrl, key) {
  const PAGE     = 1000
  const snapshot = new Map()
  let   offset   = 0
  let   page     = 1

  for (;;) {
    vlog(`  CRM страница ${page}: offset=${offset}`)
    const rows = await supaGet(
      supaUrl, key,
      'inventory_items',
      `select=id,internal_number,name,selling_price,status,last_seen_at,side,drom_url,drom_photo_id&limit=${PAGE}&offset=${offset}`
    )
    vlog(`    → получено: ${rows.length}`)
    if (!rows.length) break
    for (const r of rows) {
      snapshot.set(r.internal_number, {
        id:            r.id,
        name:          r.name,
        selling_price: r.selling_price,
        status:        r.status,
        last_seen_at:  r.last_seen_at,
        side:          r.side ?? null,
        drom_url:      r.drom_url ?? null,
        drom_photo_id: r.drom_photo_id ?? null,
      })
    }
    if (rows.length < PAGE) break
    offset += PAGE
    page++
  }

  if (snapshot.size === 0) {
    console.warn(
      '  ⚠️  CRM вернула 0 позиций — проверьте:\n' +
      '     • используется SERVICE_ROLE_KEY (не anon)\n' +
      '     • таблица inventory_items не пуста\n' +
      '     • SUPABASE_URL корректный'
    )
  }
  return snapshot
}

// ── Phase 4: Import new items ─────────────────────────────────────────────────

// Complete deferred parent-child links (from /shipments/new, see pending_parent_links)
// touching any of the internal_numbers just inserted — as either the child side
// (parent may already exist) or the parent side (children may already exist).
// pending_parent_links is expected to stay small (a handful of unresolved manual
// selections at most), so it's fetched in full rather than filtered by a large
// IN-list — avoids building huge query strings for 500-item insert batches.
// Best-effort: never throws, a failure here must not interrupt the sync.
async function resolvePendingParentLinks(supaUrl, key, newNumbers) {
  try {
    const links = await supaGet(
      supaUrl, key,
      'pending_parent_links',
      'select=id,child_internal_number,parent_internal_number'
    )
    if (!links.length) return

    const newSet = new Set(newNumbers)
    const relevant = links.filter(
      l => newSet.has(l.child_internal_number) || newSet.has(l.parent_internal_number)
    )
    if (!relevant.length) return

    const neededNumbers = new Set()
    for (const l of relevant) {
      neededNumbers.add(l.child_internal_number)
      neededNumbers.add(l.parent_internal_number)
    }

    const idRows = await supaGet(
      supaUrl, key,
      'inventory_items',
      `select=id,internal_number&internal_number=in.(${Array.from(neededNumbers).join(',')})`
    )
    const idByNumber = new Map(idRows.map(r => [r.internal_number, r.id]))

    const resolvedIds = []
    for (const l of relevant) {
      const childId  = idByNumber.get(l.child_internal_number)
      const parentId = idByNumber.get(l.parent_internal_number)
      if (!childId || !parentId) continue // one side still missing — leave the row as-is
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${childId}`, { parent_item_id: parentId })
      resolvedIds.push(l.id)
    }

    if (resolvedIds.length > 0) {
      await supaDelete(supaUrl, key, 'pending_parent_links', `id=in.(${resolvedIds.join(',')})`)
      console.log(`  ✓ Довершено отложенных родитель-потомок связей: ${resolvedIds.length}`)
    }
  } catch (e) {
    console.warn(`  ⚠️  Не удалось довершить pending_parent_links: ${e.message}`)
  }
}

async function importNewItems(supaUrl, key, newItems, crmSnapshot) {
  if (!newItems.length) return { count: 0, slashLinked: 0, pendingResolved: 0 }
  const BATCH = 500
  let slashLinked = 0
  let pendingResolved = 0

  // Загрузить все неразрешённые pending_purchases для merge
  let pendingMap = new Map()
  try {
    const pendingRows = await supaGet(
      supaUrl, key,
      'pending_purchases',
      'select=id,internal_number,purchase_price,shipment_id,chassis_code,side,receipt_status&resolved_at=is.null'
    )
    for (const r of pendingRows) {
      if (!pendingMap.has(r.internal_number)) {
        pendingMap.set(r.internal_number, r)
      }
    }
    if (pendingMap.size > 0) {
      console.log(`  Найдено pending_purchases для merge: ${pendingMap.size}`)
    }
  } catch (e) {
    console.warn(`  ⚠️  Не удалось загрузить pending_purchases: ${e.message}`)
  }

  const resolvedPendingIds = []

  for (let i = 0; i < newItems.length; i += BATCH) {
    const batch = newItems.slice(i, i + BATCH).map(item => {
      // All optional fields initialised to null so every object in the batch
      // has the same key set — PostgREST PGRST102 requires uniform keys.
      const row = {
        internal_number: item.internalNumber,
        name:            item.name,
        purchase_price:  null,
        shipment_id:     null,
        status:          'on_drom',
        selling_price:   item.dromPrice ?? null,
        chassis_code:    null,
        side:            null,
        notes:           null,
        slash_base_id:   null,
        drom_url:        item.url ?? null,
        // Позиция синка рождается сразу в on_drom, минуя приёмку (единственный путь,
        // где in_stock_since проставлялся) — момент появления на Дроме и есть «в наличии».
        in_stock_since:  new Date().toISOString(),
      }

      // Merge из pending_purchases: Дром авторитетен для name/side (парсинг),
      // pending авторитетен для purchase_price/shipment_id/chassis_code
      const pending = pendingMap.get(item.internalNumber)
      if (pending) {
        row.purchase_price = pending.purchase_price
        row.shipment_id    = pending.shipment_id
        // chassis_code: Дром не даёт отдельно — берём из pending
        if (pending.chassis_code) row.chassis_code = pending.chassis_code
        // side: Дром (parseItemSide) авторитетнее; если не распознался — берём из pending
        const parsedSide = parseItemSide(item.name)
        if (parsedSide)      row.side = parsedSide
        else if (pending.side) row.side = pending.side
        // Если при физической приёмке позиция была списана — создать как written_off
        if (pending.receipt_status === 'written_off') {
          row.status = 'written_off'
          row.notes  = '[Списано при приёмке партии]'
        }
        resolvedPendingIds.push(pending.id)
        pendingResolved++
      } else {
        // Без pending: попробовать распознать side из имени Дрома
        const parsedSide = parseItemSide(item.name)
        if (parsedSide) row.side = parsedSide
      }

      // Слэш-вариант (например 40242/1): найти базовую позицию в снапшоте
      if (item.internalNumber.includes('/')) {
        const baseNumber = item.internalNumber.split('/')[0]
        const base = crmSnapshot.get(baseNumber)
        if (base) {
          row.slash_base_id = base.id
          slashLinked++
        }
      }
      return row
    })

    if (!DRY_RUN) {
      await supaPost(
        supaUrl, key,
        'inventory_items',
        batch,
        'resolution=ignore-duplicates,return=minimal'
      )
      await resolvePendingParentLinks(supaUrl, key, batch.map(r => r.internal_number))
    } else {
      const pendingInBatch = batch.filter(r => r.purchase_price !== null).length
      console.log(`  (DRY RUN) Batch ${Math.floor(i / BATCH) + 1}: ${batch.length} новых, из них с pending: ${pendingInBatch}`)
    }
  }

  // Отметить разрешённые pending_purchases
  if (!DRY_RUN && resolvedPendingIds.length > 0) {
    const now = new Date().toISOString()
    for (const pendingId of resolvedPendingIds) {
      try {
        await supaPatch(supaUrl, key, 'pending_purchases', `id=eq.${pendingId}`, { resolved_at: now })
      } catch (e) {
        console.warn(`  ⚠️  Не удалось разрешить pending_purchase ${pendingId}: ${e.message}`)
      }
    }
    console.log(`  ✓ Разрешено pending_purchases: ${resolvedPendingIds.length}`)
  } else if (DRY_RUN && resolvedPendingIds.length > 0) {
    console.log(`  (DRY RUN) Pending к разрешению: ${resolvedPendingIds.length}`)
  }

  return { count: newItems.length, slashLinked, pendingResolved }
}

// ── Phase 5: Update existing items ────────────────────────────────────────────

async function updateExistingItems(supaUrl, key, dromItems, crmSnapshot, coverBudget) {
  const priceHistoryBatch = []
  const priceUpdates      = []
  const nameUpdates       = []
  const sideUpdates       = []
  const sideMismatches    = []
  const statusUpdates     = []
  const dromUrlUpdates    = []
  const seenNumbers       = []
  const coverCandidates   = []   // { itemId, status, photoId, savedPhotoId } — обложки тянем после DB-апдейтов
  const dromPhotoIdUpdates = []  // { id, nid } — записать последний увиденный id обложки (при смене)

  // Статусы, при появлении объявления переводимые в on_drom. Позиция могла быть
  // материализована при приёмке (in_stock) или ещё сырой (processing) — Дром делает
  // её «выставленной». Терминальные (sold/written_off/returned) и уже on_drom/reserved
  // не трогаем.
  const TO_ON_DROM = ['in_stock', 'processing']

  for (const dromItem of dromItems) {
    const crm = crmSnapshot.get(dromItem.internalNumber)
    if (!crm) continue   // new item, handled by importNewItems

    seenNumbers.push(dromItem.internalNumber)

    if (dromItem.dromPrice != null && crm.selling_price !== dromItem.dromPrice) {
      priceHistoryBatch.push({
        item_id:   crm.id,
        old_price: crm.selling_price,
        new_price: dromItem.dromPrice,
        source:    'drom_sync',
      })
      priceUpdates.push({ id: crm.id, newPrice: dromItem.dromPrice })
    }

    if (dromItem.name && dromItem.name !== crm.name) {
      nameUpdates.push({ id: crm.id, newName: dromItem.name })
    }

    // drom_url: заполняем, если пусто или отличается. Не перетираем прочие поля —
    // патчим ровно drom_url (тот же принцип сохранения, что и для статуса выше).
    if (dromItem.url && crm.drom_url !== dromItem.url) {
      dromUrlUpdates.push({ id: crm.id, newUrl: dromItem.url })
    }

    // Появление на Дроме: материализованная/сырая позиция становится on_drom.
    // ВАЖНО: трогаем только сам статус — processed_at/processed_by/started_by/
    // released_to_queue_at/фото/purchase_price/shipment_id остаются как есть,
    // работа специалиста не затирается.
    if (TO_ON_DROM.includes(crm.status)) {
      statusUpdates.push({ id: crm.id })
    }

    // Side auto-fill and mismatch detection — для активных и выставляемых на Дром
    // (включая только что материализованные in_stock/processing).
    if (crm.status === 'on_drom' || crm.status === 'reserved' || TO_ON_DROM.includes(crm.status)) {
      const parsedSide = parseItemSide(dromItem.name)
      if (parsedSide !== null) {
        if (crm.side === null) {
          sideUpdates.push({ id: crm.id, newSide: parsedSide })
        } else if (crm.side !== parsedSide) {
          // «Дром менее конкретен»: сторона с Дрома — база той же оси, что в CRM (Дром дал
          // 'F', CRM уточняет 'F.R'; или 'F.R' против 'F.R ↑ Верх'). Противоречия по стороне
          // нет — Дром просто не указал L/R или верх/низ. Не флажим как расхождение (это был
          // основной источник ложных задач в сверке). Обратный случай — Дром КОНКРЕТНЕЕ или
          // сторона реально другая (F.R vs F.L, R.R vs F.R) — по-прежнему флажим.
          const dromLessSpecific =
            crm.side.startsWith(parsedSide) && crm.side.length > parsedSide.length
          if (!dromLessSpecific) {
            sideMismatches.push({
              inventory_item_id: crm.id,
              mismatch_type:     'side_mismatch',
              detail:            { crm_side: crm.side, drom_side: parsedSide },
            })
          }
        }
      }
    }

    // Последний увиденный id обложки Дрома пишем ВСЕГДА (для любого статуса), но только
    // когда значение изменилось — иначе лишний UPDATE. savedPhotoId (старое значение из
    // снапшота) отдаём в syncCoverForItem для детекта смены фото у позиций со своими фото.
    if (dromItem.photoId) {
      const nid = Number(dromItem.photoId)
      if (Number.isFinite(nid) && nid !== (crm.drom_photo_id ?? null)) {
        dromPhotoIdUpdates.push({ id: crm.id, nid })
      }
    }

    // Кандидат на обложку: есть PHOTO_ID и позиция не в терминальном статусе.
    // Само скачивание/заливка — ниже, после DB-апдейтов (по одному, с try/catch).
    if (dromItem.photoId && !COVER_SKIP_STATUS.has(crm.status)) {
      coverCandidates.push({
        itemId:       crm.id,
        status:       crm.status,
        photoId:      dromItem.photoId,
        savedPhotoId: crm.drom_photo_id ?? null,
      })
    }
  }

  let coversLoaded     = 0
  let coversUpdated    = 0
  let coversSkippedOwn = 0
  let coverErrors      = 0
  const coverDeferred  = new Set()   // id позиций, чья обложка отложена (лимит/ошибка) — id не двигаем

  if (!DRY_RUN) {
    const now   = new Date().toISOString()
    const BATCH = 500

    // Batch update last_seen_at for all items seen in this block
    for (let i = 0; i < seenNumbers.length; i += BATCH) {
      const batch  = seenNumbers.slice(i, i + BATCH)
      const inList = batch.join(',')
      await supaPatch(supaUrl, key, 'inventory_items', `internal_number=in.(${inList})`, {
        last_seen_at: now,
      })
    }

    // Insert price_history records
    if (priceHistoryBatch.length > 0) {
      for (let i = 0; i < priceHistoryBatch.length; i += BATCH) {
        await supaPost(supaUrl, key, 'price_history', priceHistoryBatch.slice(i, i + BATCH), 'return=minimal')
      }
    }

    // Patch selling_price for changed items
    for (const { id, newPrice } of priceUpdates) {
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${id}`, { selling_price: newPrice })
    }

    // Patch names for changed items
    for (const { id, newName } of nameUpdates) {
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${id}`, { name: newName })
    }

    // Fill side where CRM has null (safe write — no mismatch flagged)
    for (const { id, newSide } of sideUpdates) {
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${id}`, { side: newSide })
    }

    // Перевод in_stock/processing → on_drom (только сам статус)
    for (const { id } of statusUpdates) {
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${id}`, { status: 'on_drom' })
    }

    // Заполнить/обновить drom_url (только это поле)
    for (const { id, newUrl } of dromUrlUpdates) {
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${id}`, { drom_url: newUrl })
    }

    // drom_photo_id записывается НИЖЕ — после обработки обложек (см. блок в конце), чтобы не
    // проставить id позициям, чья обложка отложена из-за исчерпанного лимита/ошибки.

    // Insert side_mismatch records via RPC: ON CONFLICT DO NOTHING on partial unique index.
    if (sideMismatches.length > 0) {
      await supaPost(supaUrl, key, 'rpc/upsert_drom_mismatches', { rows: sideMismatches })
    }

    // Обложки — только если COVERS_ENABLED. При выключенном тумблере на Дром за картинками не
    // ходим совсем (тогда coverDeferred пуст → drom_photo_id ниже пишется всем, как и раньше).
    // Ошибка одной картинки НЕ роняет прогон. Считаем ТОЛЬКО реальные скачивания (loaded/updated)
    // и списываем их из ОБЩЕГО НА ПРОГОН бюджета coverBudget.remaining (передан из onPageComplete,
    // не сбрасывается между страницами). Между скачиваниями — пауза COVER_DOWNLOAD_DELAY_MS.
    // Позиции, до которых бюджет не дошёл или чьё скачивание упало, кладём в coverDeferred —
    // им drom_photo_id НЕ проставляем, чтобы вернуться за обложкой на следующем круге.
    if (COVERS_ENABLED) {
      let budgetExhausted = false
      for (const c of coverCandidates) {
        if (coverBudget.remaining <= 0) {
          budgetExhausted = true
          coverDeferred.add(c.itemId)   // лимит исчерпан → обложку не тянем, id не двигаем
          continue
        }
        try {
          const r = await syncCoverForItem(supaUrl, key, c.itemId, c.status, c.photoId, c.savedPhotoId)
          if (r === 'loaded' || r === 'updated') {
            if (r === 'loaded') coversLoaded++
            else                coversUpdated++
            coverBudget.remaining--
            await sleep(COVER_DOWNLOAD_DELAY_MS)   // пауза только между картинками, обход каталога не трогаем
          } else if (r === 'skipped_own') {
            coversSkippedOwn++
          }
          // 'skipped'/'skipped_own' — обложка разрешена по правилу, id проставим ниже.
        } catch (e) {
          coverErrors++
          coverDeferred.add(c.itemId)   // скачивание упало → id не двигаем, повторим на следующем круге
          vwarn(`  ⚠️  Обложка item ${c.itemId} (photo ${c.photoId}): ${e.message}`)
        }
      }
      if (budgetExhausted) {
        console.log(`  Лимит обложек за прогон (${MAX_COVERS_PER_RUN}) достигнут — остальное на следующий круг`)
      }
    }

    // Записать последний увиденный id обложки Дрома — ТОЛЬКО тем позициям, чья обложка в этом
    // прогоне разрешена: скачана, либо осознанно пропущена по правилу (свои фото / терминальный
    // статус / purged_at / обложка уже есть). Отложенным (исчерпан лимит или ошибка скачивания)
    // id НЕ проставляем — иначе на следующем круге знакомый id заставил бы синк пропустить их
    // навсегда, и позиция осталась бы без обложки.
    for (const { id, nid } of dromPhotoIdUpdates) {
      if (coverDeferred.has(id)) continue
      await supaPatch(supaUrl, key, 'inventory_items', `id=eq.${id}`, { drom_photo_id: nid })
    }
  } else {
    if (coverCandidates.length > 0) {
      console.log(`  (DRY RUN) Кандидатов на обложку: ${coverCandidates.length}`)
    }
    if (priceUpdates.length > 0) {
      console.log(`  (DRY RUN) Изменений цен: ${priceUpdates.length}`)
    }
    if (nameUpdates.length > 0) {
      console.log(`  (DRY RUN) Изменений названий: ${nameUpdates.length}`)
    }
    if (sideUpdates.length > 0) {
      console.log(`  (DRY RUN) Сторон дописано: ${sideUpdates.length}`)
    }
    if (statusUpdates.length > 0) {
      console.log(`  (DRY RUN) Переводов in_stock/processing → on_drom: ${statusUpdates.length}`)
    }
    if (dromUrlUpdates.length > 0) {
      console.log(`  (DRY RUN) Обновлений drom_url: ${dromUrlUpdates.length}`)
    }
    if (sideMismatches.length > 0) {
      console.log(`  (DRY RUN) Расхождений стороны: ${sideMismatches.length}`)
    }
  }

  return {
    seenCount:      seenNumbers.length,
    priceUpdates:   priceUpdates.length,
    nameUpdates:    nameUpdates.length,
    sideUpdates:    sideUpdates.length,
    statusUpdates:  statusUpdates.length,
    dromUrlUpdates: dromUrlUpdates.length,
    sideMismatches: sideMismatches.length,
    coversLoaded,
    coversUpdated,
    coversSkippedOwn,
    coverErrors,
  }
}

// ── Phase 6: Counter protection ───────────────────────────────────────────────

async function protectCounter(supaUrl, key) {
  const settingRows    = await supaGet(supaUrl, key, 'app_settings', 'select=value&key=eq.next_internal_number')
  const counterBefore  = settingRows[0]?.value ?? '58100'
  const currentCounter = parseInt(counterBefore, 10)

  const PAGE  = 1000
  let   maxNum = 0
  let   offset = 0

  for (;;) {
    const rows = await supaGet(supaUrl, key, 'inventory_items', `select=internal_number&limit=${PAGE}&offset=${offset}`)
    if (!rows.length) break
    for (const r of rows) {
      const n = parseInt(r.internal_number, 10)
      if (!isNaN(n) && n > maxNum) maxNum = n
    }
    if (rows.length < PAGE) break
    offset += PAGE
  }

  let counterAfter = counterBefore
  if (maxNum >= currentCounter) {
    counterAfter = String(maxNum + 1)
    console.log('\n⚠️  СЧЁТЧИК: next_internal_number обновлён')
    vlog(`   ${counterBefore} → ${counterAfter} (MAX в БД: ${maxNum})`)
    if (!DRY_RUN) {
      await supaPatch(supaUrl, key, 'app_settings', 'key=eq.next_internal_number', { value: counterAfter })
    } else {
      console.log('   (DRY RUN — PATCH пропущен)')
    }
  } else {
    console.log('\n✅ СЧЁТЧИК: без изменений')
    vlog(`   next=${counterBefore}, MAX в БД=${maxNum}`)
  }

  return { counterBefore, counterAfter }
}

// ── Phase 7: Detect mismatches (DB query against cycleStart) ──────────────────
//
// Runs only when a block reaches the natural end of the catalog (completedFull=true).
// Relies on last_seen_at timestamps updated incrementally across all blocks of the cycle.
//   missing_from_drom: on_drom/reserved items whose last_seen_at predates cycleStart
//                      (not seen anywhere in the current cycle → disappeared from Drom)
//   sold_but_listed:   sold/returned/written_off items whose last_seen_at >= cycleStart
//                      (seen during this cycle → still listed despite being closed in CRM)

async function detectMismatches(supaUrl, key, cycleStart, opts = {}) {
  if (!cycleStart) {
    console.warn('  ⚠️  cycleStart не определён — пропуск обнаружения расхождений')
    return { missingFromDrom: 0, soldButListed: 0 }
  }

  // Предохранитель: не запускать детекцию, если круг завершился заметно РАНЬШЕ ожидаемого конца
  // (пройдено сильно меньше страниц, чем ceil(itemsCount/50)) — это недосканированный хвост, а
  // не реальные снятия. Иначе весь непройденный хвост ложно уехал бы в missing_from_drom.
  const { completedAtPage = null, itemsCount = null } = opts
  const expectedLast = itemsCount != null ? Math.ceil(itemsCount / 50) : null
  if (expectedLast != null && completedAtPage != null && completedAtPage < expectedLast - 1) {
    console.warn(
      `  ⚠️  Круг завершился на стр. ${completedAtPage}, ожидалось ~${expectedLast} — пройдено ` +
      `заметно меньше. Пропускаю обнаружение расхождений (вероятно недосканированный хвост).`
    )
    return { missingFromDrom: 0, soldButListed: 0, autoClosed: 0, skippedGuard: true }
  }

  const missingRows = await supaGet(
    supaUrl, key,
    'inventory_items',
    `select=id,status,last_seen_at` +
    `&status=in.(on_drom,reserved)` +
    `&last_seen_at=not.is.null` +
    `&last_seen_at=lt.${cycleStart}`
  )

  const soldRows = await supaGet(
    supaUrl, key,
    'inventory_items',
    `select=id,status,last_seen_at` +
    `&status=in.(sold,returned,written_off)` +
    `&last_seen_at=not.is.null` +
    `&last_seen_at=gte.${cycleStart}`
  )

  const missingFromDrom = missingRows.map(r => ({
    inventory_item_id: r.id,
    mismatch_type:     'missing_from_drom',
    detail:            { status: r.status, last_seen_at: r.last_seen_at },
  }))

  const soldButListed = soldRows.map(r => ({
    inventory_item_id: r.id,
    mismatch_type:     'sold_but_listed',
    detail:            { status: r.status, last_seen_at: r.last_seen_at },
  }))

  if (!DRY_RUN) {
    const all = [...missingFromDrom, ...soldButListed]
    if (all.length > 0) {
      // RPC handles ON CONFLICT (inventory_item_id, mismatch_type) WHERE (resolved=false) DO NOTHING
      // in a single transaction — partial unique index conflict no longer aborts the whole batch.
      await supaPost(supaUrl, key, 'rpc/upsert_drom_mismatches', { rows: all })
    }
  } else {
    console.log(
      `  (DRY RUN) Расхождений: пропало=${missingFromDrom.length}, продано+висит=${soldButListed.length}`
    )
  }

  // Автозакрытие: позиция, ранее помеченная missing_from_drom, снова видна в этом круге
  // (last_seen_at >= cycleStart) — объявление вернулось на Дром, расхождение исчезло само.
  // Закрываем только этот тип; side_mismatch и прочее снимает человек. Список id бьём на
  // чанки, чтобы не упереться в длину URL PostgREST.
  let autoClosed = 0
  if (!DRY_RUN) {
    const openMissing = await supaGet(
      supaUrl, key, 'drom_mismatches',
      `select=id,inventory_item_id&mismatch_type=eq.missing_from_drom&resolved=eq.false`
    )
    if (openMissing.length > 0) {
      const CHUNK = 100
      const reseenIds = new Set()
      const itemIds   = openMissing.map(m => m.inventory_item_id)
      for (let i = 0; i < itemIds.length; i += CHUNK) {
        const slice = itemIds.slice(i, i + CHUNK)
        const rows  = await supaGet(
          supaUrl, key, 'inventory_items',
          `select=id&last_seen_at=gte.${cycleStart}&id=in.(${slice.join(',')})`
        )
        for (const r of rows) reseenIds.add(r.id)
      }
      const toClose = openMissing
        .filter(m => reseenIds.has(m.inventory_item_id))
        .map(m => m.id)
      if (toClose.length > 0) {
        const nowIso = new Date().toISOString()
        for (let i = 0; i < toClose.length; i += CHUNK) {
          const slice = toClose.slice(i, i + CHUNK)
          await supaPatch(
            supaUrl, key, 'drom_mismatches',
            `id=in.(${slice.join(',')})`,
            { resolved: true, resolved_at: nowIso }
          )
        }
      }
      autoClosed = toClose.length
    }
    if (autoClosed > 0) console.log(`  Автозакрыто (снова на Дроме): ${autoClosed}`)
  }

  return {
    missingFromDrom: missingFromDrom.length,
    soldButListed:   soldButListed.length,
    autoClosed,
  }
}

// ── Phase 8: Write run log ────────────────────────────────────────────────────

async function writeRunLog(supaUrl, key, stats) {
  const row = {
    pages_scanned:           stats.pagesScanned,
    total_listings:          stats.totalListings,
    new_imported:            stats.newImported,
    unrecognized_count:      stats.unrecognizedCount,
    counter_before:          stats.counterBefore,
    counter_after:           stats.counterAfter,
    completed_full:          stats.completedFull,
    last_page_reached:       stats.lastPageReached ?? null,
    price_updates:           stats.priceUpdates,
    name_updates:            stats.nameUpdates,
    mismatches_missing:      stats.mismatchesMissing,
    mismatches_sold_listed:  stats.mismatchesSold,
    run_source:              RUN_SOURCE,
    items_count:             stats.itemsCount ?? null,
  }

  if (!DRY_RUN) {
    try {
      await supaPost(supaUrl, key, 'drom_sync_runs', row, 'return=minimal')
    } catch (e) {
      console.warn(`  ⚠️  Не удалось записать лог прогона: ${e.message}`)
    }
  }
}

// ── Write unrecognized listings to DB ────────────────────────────────────────

async function writeUnrecognized(supaUrl, key, items) {
  const withUrl = items.filter(u => u.url)
  if (!withUrl.length) return

  const now  = new Date().toISOString()
  const rows = withUrl.map(u => ({
    raw_title:    u.title,
    url:          u.url,
    last_seen_at: now,
  }))

  // INSERT via RPC: ON CONFLICT (url) WHERE (url IS NOT NULL AND url <> '') DO NOTHING
  await supaPost(supaUrl, key, 'rpc/upsert_drom_unrecognized_listings', { rows })

  // Обновить last_seen_at на нерезолюченных записях
  for (const u of withUrl) {
    try {
      await supaPatch(
        supaUrl, key,
        'drom_unrecognized_listings',
        `url=eq.${encodeURIComponent(u.url)}&resolved=eq.false`,
        { last_seen_at: now }
      )
    } catch { /* non-critical */ }
  }
}

// ── Write number collisions to DB ────────────────────────────────────────────

async function writeCollisions(supaUrl, key, collisions) {
  const now  = new Date().toISOString()
  const rows = collisions.map(col => ({
    internal_number: col.internal_number,
    url_a:           col.url_a,
    name_a:          col.name_a,
    side_a:          col.side_a,
    url_b:           col.url_b,
    name_b:          col.name_b,
    side_b:          col.side_b,
    last_seen_at:    now,
  }))

  // INSERT via RPC: ON CONFLICT (internal_number) WHERE (resolved = false) DO NOTHING
  await supaPost(supaUrl, key, 'rpc/upsert_drom_number_collisions', { rows })

  // Обновить last_seen_at на нерезолюченных записях (новых и уже существующих)
  for (const col of collisions) {
    try {
      await supaPatch(
        supaUrl, key,
        'drom_number_collisions',
        `internal_number=eq.${encodeURIComponent(col.internal_number)}&resolved=eq.false`,
        { last_seen_at: now }
      )
    } catch (e) {
      vwarn(`  ⚠️  Не удалось обновить last_seen_at для коллизии №${col.internal_number}: ${e.message}`)
    }
  }

  console.log(`  ⚠️  Коллизий номеров зафиксировано в БД: ${collisions.length}`)
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(
    `\n🔄 АвтоCRM — Drom Sync (блочный режим, ${MAX_PAGES_PER_RUN} стр./прогон)` +
    `${DRY_RUN ? ' [DRY RUN]' : ''}`
  )
  console.log('='.repeat(60))

  const { url: supaUrl, key } = loadConfig()
  vlog(`🔗 Supabase: ${supaUrl}`)

  // Pre-Phase: backoff-гейт. Если предыдущий прогон поймал 403/бан — пропускаем
  // ближайшие прогоны, не трогая Дром. Состояние в app_settings, переживает рестарт.
  const backoff = await getBackoff(supaUrl, key)
  if (backoff.skipRemaining > 0) {
    const remainingAfter = backoff.skipRemaining - 1
    const eta = nextAttemptEtaUtc(remainingAfter)
    if (DRY_RUN) {
      console.log(
        `\n⏸  (DRY RUN) backoff активен: осталось пропусков ${backoff.skipRemaining}` +
        ` (отказов подряд: ${backoff.failCount}). Реальный прогон пропустился бы,` +
        ` следующая попытка ~${eta} UTC. Продолжаю для теста.`
      )
    } else {
      await setSetting(supaUrl, key, 'drom_backoff_skip_remaining', remainingAfter)
      console.log(
        `\n⏸  backoff, пропуск ${backoff.skipRemaining},` +
        ` следующая попытка примерно в ${eta} UTC (отказов подряд: ${backoff.failCount})`
      )
      process.exit(0)
    }
  }

  // Pre-Phase: determine resume position and check 24h cooldown
  const startPage = await checkLastRun(supaUrl, key)

  // При CYCLE_COOLDOWN_HOURS === 0 пауза между кругами выключена — проверку не выполняем.
  if (startPage === 1 && CYCLE_COOLDOWN_HOURS > 0) {
    const completedAt = await getCycleCompleted(supaUrl, key)
    if (completedAt) {
      const hoursAgo = (Date.now() - new Date(completedAt).getTime()) / 3_600_000
      if (hoursAgo < CYCLE_COOLDOWN_HOURS) {
        const hoursLeft = (CYCLE_COOLDOWN_HOURS - hoursAgo).toFixed(1)
        console.log(
          `\n⏸  Круг завершён ${hoursAgo.toFixed(1)} ч. назад,` +
          ` следующий начнётся через ${hoursLeft} ч. — пропускаем прогон`
        )
        process.exit(0)
      }
    }
  }

  // Pre-Phase: read current cycle start
  let cycleStart = await getCycleStart(supaUrl, key)
  console.log(`\n⏱  Начало текущего цикла: ${cycleStart ?? '(нет — первый прогон нового цикла)'}`)

  // Phase 2 (moved before page loop): CRM snapshot needed by per-page callback
  console.log('\n📋 Загрузка снапшота CRM...')
  const crmSnapshot = await loadCrmSnapshot(supaUrl, key)
  vlog(`  В CRM: ${crmSnapshot.size} позиций`)

  // Stats accumulated across all pages in this block
  let totalNewImported     = 0
  let totalPendingResolved = 0
  let totalPriceUpdates    = 0
  let totalNameUpdates     = 0
  let totalSideUpdates     = 0
  let totalSideMismatches  = 0
  let totalSeenCount       = 0
  let totalCoversLoaded     = 0
  let totalCoversUpdated    = 0
  let totalCoversSkippedOwn = 0
  let totalCoverErrors      = 0

  // Бюджет скачивания обложек — ОБЩИЙ НА ПРОГОН. Один объект на все страницы блока, поэтому
  // потолок MAX_COVERS_PER_RUN держится за прогон, а не за страницу.
  const coverBudget = { remaining: MAX_COVERS_PER_RUN }

  // Per-page callback: diff → import → update → write cursor checkpoint.
  // Called after each page is fetched and deduplicated.
  // Errors propagate up and abort the block — cursor stays at last successfully committed page.
  const onPageComplete = async (pg, pageItems, _pageUnrecognized) => {
    const newPageItems = pageItems.filter(item => !crmSnapshot.has(item.internalNumber))

    if (newPageItems.length > 0) {
      if (!DRY_RUN) {
        const result = await importNewItems(supaUrl, key, newPageItems, crmSnapshot)
        totalNewImported     += result.count
        totalPendingResolved += result.pendingResolved
      } else {
        const slashLinkedDry = newPageItems.filter(i => {
          if (!i.internalNumber.includes('/')) return false
          return crmSnapshot.has(i.internalNumber.split('/')[0])
        }).length
        console.log(
          `  (DRY RUN) Стр.${pg}: ${newPageItems.length} новых` +
          ` (слэш со связью: ${slashLinkedDry}) — INSERT пропущен`
        )
        for (const item of newPageItems.slice(0, 5)) {
          vlog(`    • №${item.internalNumber} — ${item.name} (${item.dromPrice ?? '?'} ₽)`)
        }
        if (newPageItems.length > 5) vlog(`    ... и ещё ${newPageItems.length - 5}`)
      }
    }

    const upd = await updateExistingItems(supaUrl, key, pageItems, crmSnapshot, coverBudget)
    totalSeenCount      += upd.seenCount
    totalPriceUpdates   += upd.priceUpdates
    totalNameUpdates    += upd.nameUpdates
    totalSideUpdates    += upd.sideUpdates
    totalSideMismatches += upd.sideMismatches
    totalCoversLoaded     += upd.coversLoaded
    totalCoversUpdated    += upd.coversUpdated
    totalCoversSkippedOwn += upd.coversSkippedOwn
    totalCoverErrors      += upd.coverErrors

    if (!DRY_RUN) {
      await setLastPageReached(supaUrl, key, pg)
      console.log(
        `  ✓ Стр.${pg}: нов=${newPageItems.length}, обновл=${upd.seenCount},` +
        ` цен=${upd.priceUpdates} → ckp pg=${pg}`
      )
    }
  }

  // Phase 1: scan block (resume from last_page_reached + 1), process each page immediately
  console.log('\n📡 Обход каталога ...')
  const { allItems, allUnrecognized, pagesScanned, completedFull, lastPageReached, blockCollisions, warmupBanned, itemsCount, completedAtPage } =
    await fetchAllDromListings(supaUrl, key, startPage, onPageComplete)

  // Backoff ТОЛЬКО при бане на разогреве (Дром не пустил вообще). 403 посреди
  // блока после успешных страниц — это лимит частоты, не бан: он сюда не приходит
  // (warmupBanned=false), блок завершён штатно, чекпоинт сохранён.
  if (warmupBanned) {
    if (!DRY_RUN) {
      const newFail = backoff.failCount + 1
      const skips   = backoffSkipsFor(newFail)
      await setSetting(supaUrl, key, 'drom_backoff_fail_count',     newFail)
      await setSetting(supaUrl, key, 'drom_backoff_skip_remaining', skips)
      await setSetting(supaUrl, key, 'drom_backoff_last_403_at',    new Date().toISOString())
      console.warn(
        `\n🛑 403 на разогреве (бан). Отказов подряд: ${newFail}.` +
        ` Пропускаю следующие ${skips} прогонов, следующая попытка примерно в ${nextAttemptEtaUtc(skips)} UTC`
      )
    } else {
      console.log(`\n🛑 (DRY RUN) 403 на разогреве — реальный прогон записал бы backoff (отказ #${backoff.failCount + 1})`)
    }
    process.exit(0)
  }

  // Разогрев прошёл = успешный контакт с Дромом → сбрасываем backoff, если был
  // активен. Сюда попадаем в т.ч. когда внутри блока был 403 (лимит частоты):
  // счётчик отказов обнуляется, как и требуется.
  if (!DRY_RUN && (backoff.failCount > 0 || backoff.skipRemaining > 0)) {
    await setSetting(supaUrl, key, 'drom_backoff_fail_count',     0)
    await setSetting(supaUrl, key, 'drom_backoff_skip_remaining', 0)
    console.log('  ✓ backoff сброшен (разогрев прошёл — успешный контакт с Дромом)')
  }

  // Write number collisions detected within this block
  if (blockCollisions.length > 0 && !DRY_RUN) {
    console.log('\n⚠️  Запись коллизий номеров...')
    await writeCollisions(supaUrl, key, blockCollisions)
  } else if (blockCollisions.length > 0 && DRY_RUN) {
    console.log('\n(DRY RUN) Коллизии номеров (запись пропущена):')
    for (const col of blockCollisions) {
      vlog(`  №${col.internal_number}: "${col.name_a}" vs "${col.name_b}"`)
    }
  }

  // If this is the first block of a new cycle, record the cycle start time
  if (startPage === 1) {
    const now = new Date().toISOString()
    cycleStart = now
    if (!DRY_RUN) {
      await setCycleStart(supaUrl, key, now)
      console.log(`  Новый цикл начат: ${now}`)
    } else {
      console.log(`  (DRY RUN) Новый цикл начат: ${now}`)
    }
  }

  // On full cycle completion, clear cursor so next run starts from page 1
  if (completedFull && !DRY_RUN) {
    await setLastPageReached(supaUrl, key, null)
    console.log('  Курсор сброшен — следующий цикл начнётся со страницы 1')
  }

  if (completedFull) {
    console.log(
      `\n✅ Каталог полностью обойдён: ${allItems.length} позиций` +
      ` (+ ${allUnrecognized.length} нераспознанных)`
    )
  } else {
    console.log(
      `\n⏸  Блок завершён — страницы ${startPage}–${lastPageReached ?? startPage},` +
      ` собрано ${allItems.length} позиций`
    )
    console.log('   Расхождения будут обработаны при завершении полного круга.')
  }

  if (allUnrecognized.length > 0) {
    vlog('\n⚠️  Нераспознанные объявления (без внутреннего номера):')
    for (const u of allUnrecognized) {
      vlog(`  • ${u.title}`)
      vlog(`    https://baza.drom.ru${u.url}`)
    }
    if (!DRY_RUN) {
      await writeUnrecognized(supaUrl, key, allUnrecognized)
      console.log(`  Нераспознанных зафиксировано в БД: ${allUnrecognized.filter(u => u.url).length}`)
    }
  }

  // Phase 6: protect counter
  console.log('\n🔢 Проверка счётчика internal_number...')
  const { counterBefore, counterAfter } = await protectCounter(supaUrl, key)

  // Phase 7: detect mismatches — only when full cycle completed
  let mismatchesMissing = 0
  let mismatchesSold    = 0
  if (completedFull) {
    console.log('\n🔍 Поиск расхождений...')
    const result = await detectMismatches(supaUrl, key, cycleStart, { completedAtPage, itemsCount })
    mismatchesMissing = result.missingFromDrom
    mismatchesSold    = result.soldButListed
    console.log(`  Пропали с Дрома: ${mismatchesMissing}, продано но висит: ${mismatchesSold}, автозакрыто: ${result.autoClosed}`)

    if (!DRY_RUN) {
      const completedNow = new Date().toISOString()
      await setCycleCompleted(supaUrl, key, completedNow)
      console.log(
        CYCLE_COOLDOWN_HOURS > 0
          ? `  Круг завершён, следующий — не ранее чем через ${CYCLE_COOLDOWN_HOURS} ч. (${completedNow})`
          : `  Круг завершён, следующий начнётся сразу — пауза между кругами выключена (${completedNow})`
      )
    }
  }

  // Phase 8: write run log
  const stats = {
    pagesScanned,
    totalListings:     allItems.length + allUnrecognized.length,
    newImported:       DRY_RUN ? 0 : totalNewImported,
    unrecognizedCount: allUnrecognized.length,
    counterBefore,
    counterAfter,
    completedFull,
    lastPageReached,
    priceUpdates:      DRY_RUN ? 0 : totalPriceUpdates,
    nameUpdates:       DRY_RUN ? 0 : totalNameUpdates,
    mismatchesMissing: DRY_RUN ? 0 : mismatchesMissing,
    mismatchesSold:    DRY_RUN ? 0 : mismatchesSold,
    itemsCount:        itemsCount ?? null,
  }
  await writeRunLog(supaUrl, key, stats)

  console.log('\n' + '='.repeat(60))
  console.log('✅ Готово!')
  console.log(`   Страниц обработано   : ${stats.pagesScanned}`)
  console.log(`   Полный обход         : ${stats.completedFull ? 'да' : 'нет (блок)'}`)
  console.log(`   Последняя страница   : ${stats.lastPageReached ?? '— (конец каталога)'}`)
  console.log(`   Позиций в блоке      : ${stats.totalListings}`)
  console.log(`   Импортировано новых  : ${stats.newImported}`)
  console.log(`   Pending разрешено    : ${DRY_RUN ? 0 : totalPendingResolved}`)
  console.log(`   Цен обновлено        : ${stats.priceUpdates}`)
  console.log(`   Названий обновлено   : ${stats.nameUpdates}`)
  console.log(`   Нераспознано         : ${stats.unrecognizedCount}`)
  console.log(`   Расхождений (пропало): ${stats.mismatchesMissing}`)
  console.log(`   Расхождений (продано): ${stats.mismatchesSold}`)
  console.log(`   Сторон дописано      : ${DRY_RUN ? 0 : totalSideUpdates}`)
  console.log(`   Расхождений (сторона): ${DRY_RUN ? 0 : totalSideMismatches}`)
  console.log(`   Коллизий номеров     : ${blockCollisions.length}`)
  if (COVERS_ENABLED) {
    console.log(`   Обложки загр/обновл/своифото/ошибок : ${DRY_RUN ? 0 : totalCoversLoaded} / ${DRY_RUN ? 0 : totalCoversUpdated} / ${DRY_RUN ? 0 : totalCoversSkippedOwn} / ${DRY_RUN ? 0 : totalCoverErrors}`)
  } else {
    console.log(`   Обложки              : ОТКЛЮЧЕНЫ (COVERS_ENABLED=false) — не качаются; drom_photo_id пишется`)
  }
  vlog(`   Счётчик до           : ${stats.counterBefore}`)
  vlog(`   Счётчик после        : ${stats.counterAfter}`)
}

// Экспорт для переиспользования в scripts/drom-pending-fetch.mjs (добор позиций по номерам
// из pending_purchases через поиск Дрома). main() запускается ТОЛЬКО при прямом вызове файла
// (гвард ниже), поэтому импорт из другого скрипта НЕ триггерит полный синк.
export {
  loadConfig, warmupSession, parseFeed, extractInternalNumber, parseItemSide,
  loadCrmSnapshot, importNewItems, supaGet, supaPost, supaPatch,
  buildCookieHeader, extractCookies, charsetFromContentType, isInterstitial, setSetting,
  BASE_URL, UA,
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(e => {
    console.error('\n❌ Fatal:', e.message)
    process.exit(1)
  })
}
