// server.js — Real-time parking (City of Melbourne Opendatasoft) + basic historical stats
// Run: npm i && node server.js
// Env (.env): see .env.sample
require('dotenv').config();
const { aggregateSensorsToAreas, rankAreas } = require('./lib/areas');
const { haversineMeters } = require('./lib/geo');
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const path = require('path');


dotenv.config();

const { fetchLiveSensors, fetchEventsHourlyCounts } = require('./lib/opendatasoft');

const app = express();
const PORT = process.env.PORT || 4000;
app.use(cors());
// 静态托管前端
app.use(express.static(path.join(__dirname, '..', 'frontend')));

// 根路径返回 index.html
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'frontend', 'index.html'));
});

// --- In-memory hourly z per parking area (fallback when EVENTS_DATASET_ID has no usable data)
const AREA_HOURLY = new Map(); // key: areaId -> Map(tsISO -> { ts, free, occ, total, n })

function hourISO(d = new Date()) {
  const t = new Date(d);
  t.setUTCMinutes(0, 0, 0);
  return t.toISOString();
}
function _pruneOld(buckets, days = 30) {
  const cut = Date.now() - days * 24 * 3600 * 1000;
  for (const [k] of buckets) {
    if (new Date(k).getTime() < cut) buckets.delete(k);
  }
}
function recordAreaSample(areaId, { free = 0, occ = 0, total = 0, ts = new Date() } = {}) {
  if (!areaId) return;
  const key = hourISO(ts);
  let buckets = AREA_HOURLY.get(areaId);
  if (!buckets) {
    buckets = new Map();
    AREA_HOURLY.set(areaId, buckets);
  }
  const b = buckets.get(key) || { ts: key, free: 0, occ: 0, total: 0, n: 0 };
  b.free += Number(free) || 0;
  b.occ += Number(occ) || 0;
  b.total += Number(total) || 0;
  b.n += 1;
  buckets.set(key, b);
  _pruneOld(buckets, 30);
}
function readAreaSeries(areaId, days = 14) {
  const buckets = AREA_HOURLY.get(areaId);
  if (!buckets) return [];
  const cut = Date.now() - days * 24 * 3600 * 1000;
  return Array.from(buckets.values())
    .filter(b => new Date(b.ts).getTime() >= cut)
    .sort((a, b) => a.ts.localeCompare(b.ts))
    .map(b => ({
      ts: b.ts,
      free: Math.round(b.free / Math.max(1, b.n)),
      occ: Math.round(b.occ / Math.max(1, b.n)),
      total: Math.round(b.total / Math.max(1, b.n))
    }));
}
function forecastFromSeries(series, hours = 24) {
  if (!series || !series.length) return [];
  const last = series[series.length - 1];
  // Simple hour-of-day seasonal average
  const pattern = new Map();
  if (series.length >= 24) {
    const grp = {};
    for (const s of series) {
      const h = new Date(s.ts).getUTCHours();
      (grp[h] || (grp[h] = [])).push(s.free);
    }
    for (const h of Object.keys(grp)) {
      const arr = grp[h];
      pattern.set(Number(h), Math.round(arr.reduce((a, v) => a + v, 0) / arr.length));
    }
  }
  const out = [];
  for (let i = 1; i <= hours; i++) {
    const d = new Date(last.ts);
    d.setUTCHours(d.getUTCHours() + i);
    const h = d.getUTCHours();
    let free = pattern.has(h) ? pattern.get(h) : last.free;
    let total = last.total || (last.free + last.occ);
    if (free > total) free = total;
    if (free < 0) free = 0;
    out.push({ ts: d.toISOString(), free, occ: Math.max(0, total - free), total });
  }
  return out;
}

// --- Forecast helpers: hour-of-day (HOD) profile with empirical quantiles
function _quantile(sortedArr, q) {
  if (!sortedArr || !sortedArr.length) return null;
  const a = sortedArr;
  const pos = (a.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (a[base + 1] !== undefined) return a[base] + rest * (a[base + 1] - a[base]);
  return a[base];
}
function buildHodProfile(rows) {
  // rows: [{ts, free, occ, total}]
  const buckets = Array.from({ length: 24 }, () => []);
  const totals  = [];
  for (const r of (rows || [])) {
    const t = (typeof r.ts === 'string' || r.ts instanceof Date) ? new Date(r.ts) : null;
    const h = t && !isNaN(t) ? t.getUTCHours() : null;
    const tot = Number(r.total || 0);
    const free = Number(r.free || 0);
    if (h === null || h < 0 || h > 23) continue;
    const denom = tot > 0 ? tot : Math.max(1, free + Number(r.occ || 0));
    const fr = Math.max(0, Math.min(1, free / denom));
    buckets[h].push(fr);
    if (tot > 0) totals.push(tot);
  }
  const mean = new Array(24).fill(0.5);
  const q10  = new Array(24).fill(0.25);
  const q90  = new Array(24).fill(0.9);
  for (let h = 0; h < 24; h++) {
    const arr = buckets[h].slice().sort((a,b)=>a-b);
    if (arr.length) {
      const m = arr.reduce((s,v)=>s+v, 0) / arr.length;
      mean[h] = Math.max(0, Math.min(1, m));
      const lo = _quantile(arr, 0.10);
      const hi = _quantile(arr, 0.90);
      if (lo !== null) q10[h] = Math.max(0, Math.min(1, lo));
      if (hi !== null) q90[h] = Math.max(0, Math.min(1, hi));
    }
  }
  const totalFromRows = totals.length ? Math.max(...totals) : 0;
  return { mean, q10, q90, totalFromRows };
}
function forecastNextHoursFromHod({ hours = 24, baseTotal = 0, mean, q10, q90 }) {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours() + 1));
  const out = [];
  for (let i = 0; i < hours; i++) {
    const t = new Date(start.getTime() + i * 3600 * 1000);
    const hod = t.getUTCHours();
    const fr  = Math.min(1, Math.max(0, mean[hod] ?? 0.5));
    const frLo = Math.min(1, Math.max(0, q10[hod] ?? (fr * 0.8)));
    const frHi = Math.min(1, Math.max(0, q90[hod] ?? Math.min(1, fr * 1.1)));
    const muAvail = fr * baseTotal;
    out.push({
      ts: t.toISOString(),
      expected_available: Math.round(muAvail),
      lo80: Math.max(0, Math.round(frLo * baseTotal)),
      hi80: Math.min(baseTotal, Math.round(frHi * baseTotal)),
      free_ratio: fr
    });
  }
  return out;
}

function toNumber(v, def=null){
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function mapSensorRecordToParking(r){
    const lat = Number(r.lat ?? r.latitude ?? (r.location && r.location.lat) ?? (r.geo_point_2d && r.geo_point_2d.lat));
    const lon = Number(r.lng ?? r.lon ?? r.longitude ?? (r.location && r.location.lon) ?? (r.geo_point_2d && r.geo_point_2d.lon));
    const id  = String(r.kerbsideid ?? r.bay_id ?? r.bayid ?? r.marker_id ?? r.sensor ?? r.asset_id ?? r.device_id ?? r.id ?? r.recordid ?? `${lat},${lon}`);
    const name = r.name || (r.kerbsideid ? `Kerbside ${r.kerbsideid}` : (r.marker_id ? `Marker ${r.marker_id}` : (r.bay_id ? `Bay ${r.bay_id}` : `Sensor ${id}`)));

    // Infer availability (true=free, false=occupied, null=unknown)
    const candidates = [
      r.status_description, r.status, r.Status, r.occupancy, r.occupancy_status, r.occupancystatus, r.parkingstatus,
      r.bay_status, r.baystatus, r.vehicle_present, r.vehiclepresent, r.present, r.presence,
      r.is_occupied, r.isoccupied, r.is_free, r.isfree, r.available, r.availability, r.occupied
    ];
    let val = candidates.find(v => v !== undefined && v !== null);
    let availability = null;
    if (typeof val === 'string') {
      const s = val.trim().toLowerCase();
      if (['vacant','unoccupied','free','available','clear','unocc','empty','not present','no','0'].includes(s)) availability = true;
      else if (['occupied','present','busy','full','yes','1','true'].includes(s)) availability = false;
      else if (s.includes('unoccupied') || s.includes('vacant') || s.includes('available')) availability = true;
      else if (s.includes('occupied') || s.includes('present')) availability = false;
    } else if (typeof val === 'boolean') {
      // Most feeds: is_occupied/vehicle_present = true -> occupied (not free)
      availability = (val === true) ? false : true;
    } else if (typeof val === 'number') {
      // 0 = free, 1 = occupied (common)
      if (val === 0) availability = true;
      else if (val === 1) availability = false;
    }

    const updated = r.status_timestamp || r.lastupdated || r.last_update || r.updated_at || r.modificationdate || r.datetime || r.timestamp || new Date().toISOString();

    // available_spots: keep null when unknown so UI can show "—" instead of 0
    const available_spots = (availability === true) ? 1 : ((availability === false) ? 0 : null);

    return { id, name, lat, lng: lon, capacity: 1, available_spots, price: null, updated_at: updated, raw_status: val };
}
// Helper: resolve area members (parking spaces belonging to an area)
async function resolveAreaMembers(areaId, { lat, lng, radiusMeters, resolution }) {
  const h3 = require('h3-js');
  const toCell   = h3.latLngToCell   || h3.geoToH3;
  const toLatLng = h3.cellToLatLng   || h3.h3ToGeo;
  const getRes   = h3.getResolution  || h3.h3GetResolution;
  const isValid  = h3.isValidCell    || h3.h3IsValid;

  if (!isValid || !isValid(areaId)) {
    throw new Error(`Invalid H3 areaId: ${areaId}`);
  }

  // Build a list of candidate resolutions to try (coerced & deduped)
  const coerce = (x) => Number.isFinite(x) ? Math.floor(Number(x)) : NaN;
  const fromParam = coerce(resolution);
  const fromId    = coerce(typeof getRes === 'function' ? getRes(areaId) : NaN);
  const candidates = [];
  if (Number.isInteger(fromParam) && fromParam >= 0 && fromParam <= 15) candidates.push(fromParam);
  if (Number.isInteger(fromId)    && fromId    >= 0 && fromId    <= 15) candidates.push(fromId);
  // Add some safe defaults to avoid code:4
  candidates.push(9, 8, 10);
  // Dedupe while preserving order
  const seen = new Set();
  const resList = candidates.filter(r => {
    if (!Number.isInteger(r) || r < 0 || r > 15) return false;
    if (seen.has(r)) return false; seen.add(r); return true;
  });

  function tryToCell(lat0, lng0) {
    for (const r of resList) {
      try {
        const c = toCell(lat0, lng0, r);
        return { cell: c, res: r };
      } catch (e) {
        // code:4 => bad resolution; try next candidate
        if (e && (e.code === 4 || String(e.message||'').includes('Resolution'))) continue;
        continue;
      }
    }
    return null;
  }

  // First pass around provided lat/lng
  let recs   = await fetchLiveSensors({ lat, lng, radiusMeters, limit: 3000 });
  let items  = recs.map(mapSensorRecordToParking);
  let resolvedRes = resList[0] ?? 9;
  let members = [];
  for (const p of items) {
    if (!Number.isFinite(p.lat) || !Number.isFinite(p.lng)) continue;
    const t = tryToCell(p.lat, p.lng);
    if (t && t.cell === areaId) { members.push(p); resolvedRes = t.res; }
  }

  // Fallback: fetch around the hex center if nothing matched
  if (members.length === 0) {
    const [cLat, cLng] = toLatLng(areaId);
    const fallbackRadius = Math.max(600, Math.min(2000, radiusMeters || 1200));
    recs   = await fetchLiveSensors({ lat: cLat, lng: cLng, radiusMeters: fallbackRadius, limit: 3000 });
    items  = recs.map(mapSensorRecordToParking);
    for (const p of items) {
      if (!Number.isFinite(p.lat) || !Number.isFinite(p.lng)) continue;
      const t = tryToCell(p.lat, p.lng);
      if (t && t.cell === areaId) { members.push(p); resolvedRes = t.res; }
    }
  }

  return { members, resolution: resolvedRes };
}

// Debug: sample occupancy status values near a point
app.get('/api/v1/debug/occupancy', async (req, res) => {
    try {
        const lat = Number(req.query.lat ?? -37.8136);
        const lng = Number(req.query.lng ?? 144.9631);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const limit = Number(req.query.limit ?? 400);
        const recs = await fetchLiveSensors({ lat, lng, radiusMeters, limit });
        const hist = {};
        const pick = (r) => r.status_description ?? r.status ?? r.occupancy_status ?? r.occupancystatus ?? r.bay_status ?? r.baystatus ?? r.vehicle_present ?? r.vehiclepresent ?? r.is_occupied ?? r.is_free;
        const sample = [];
        for (const r of recs) {
            const raw = pick(r);
            const key = (raw === undefined || raw === null) ? 'undefined' : String(raw).trim().toLowerCase();
            hist[key] = (hist[key] || 0) + 1;
            if (sample.length < 30) sample.push({ raw_status: raw, id: r.kerbsideid ?? r.bay_id ?? r.id ?? r.objectid, lat: r.lat ?? r.latitude, lon: r.lon ?? r.lng ?? r.longitude });
        }
        res.json({ total: recs.length, statusHistogram: hist, sample });
    } catch (err) {
        console.error('GET /debug/occupancy error', err);
        res.status(500).json({ error: 'Failed to sample occupancy', detail: String(err.message || err) });
    }
});

// Debug: return raw sample records (keys + first few records) to verify available fields
app.get('/api/v1/debug/raw-sample', async (req, res) => {
  try {
    const lat = Number(req.query.lat ?? -37.8136);
    const lng = Number(req.query.lng ?? 144.9631);
    const radiusMeters = Number(req.query.radius ?? 1200);
    const limit = Number(req.query.limit ?? 5);
    const recs = await fetchLiveSensors({ lat, lng, radiusMeters, limit });
    const keys = recs.length ? Object.keys(recs[0]) : [];
    res.json({ count: recs.length, keys, sample: recs.slice(0, Math.min(limit, 10)) });
  } catch (err) {
    console.error('GET /debug/raw-sample error', err);
    res.status(500).json({ error: 'Failed to fetch raw sample', detail: String(err.message || err) });
  }
});

// Debug: sample records from the EVENTS_DATASET_ID (which fields exist?)
app.get('/api/v1/debug/events-sample', async (req, res) => {
    try {
        const days = Math.min(3650, Math.max(1, Number(req.query.days || 30)));
        const lat = Number(req.query.lat ?? -37.8136);
        const lng = Number(req.query.lng ?? 144.9631);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const ods = require('./lib/opendatasoft');
        if (!ods || typeof ods.fetchEventsRawSample !== 'function') {
            return res
                .status(501)
                .json({ error: 'Not implemented', detail: 'fetchEventsRawSample missing in lib/opendatasoft.js' });
        }
        const out = await ods.fetchEventsRawSample({ days, lat, lng, radiusMeters, limit: 20 });
        res.json(out);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch events sample', detail: String(err.message || err) });
    }
});
// Debug: probe annual dataset fields and sample rows
app.get('/api/v1/debug/annual-sample', async (req, res) => {
    try {
        const datasetId = String(req.query.dataset || '').trim();
        const year = Number(req.query.year || process.env.ANNUAL_DEFAULT_YEAR || 2019);
        const lat = Number(req.query.lat ?? -37.8136);
        const lng = Number(req.query.lng ?? 144.9631);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const limit = Math.min(50, Math.max(1, Number(req.query.limit || 5)));
        if (!datasetId) return res.status(400).json({ error: 'Missing dataset param' });

        const yearStart = new Date(Date.UTC(year, 0, 1, 0, 0, 0)).toISOString();
        const yearEnd = (year === 2020 && String(process.env.ANNUAL_2020_MAY_ONLY || '1') === '1')
            ? new Date(Date.UTC(2020, 4, 31, 23, 59, 59)).toISOString()
            : new Date(Date.UTC(year, 11, 31, 23, 59, 59)).toISOString();

        const ods = require('./lib/opendatasoft');
        const out = await ods.fetchAnnualRawSample({ datasetId, startISO: yearStart, endISO: yearEnd, lat, lng, radiusMeters, limit });
        res.json(out);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch annual sample', detail: String(err.message || err) });
    }
});
// --- Parking Areas (H3 aggregation) ---
// GET /api/v1/parking/areas?lat=...&lng=...&radius=1200&res=9&limit=20&sort=mix
app.get('/api/v1/parking/areas', async (req, res) => {
    try {
        const lat = Number(req.query.lat ?? -37.8136);
        const lng = Number(req.query.lng ?? 144.9631);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const limit = Math.max(1, Math.min(Number(req.query.limit ?? 20), 100));
        const resH3Raw = Number(req.query.res ?? 9);
        const resH3 = Number.isInteger(resH3Raw) && resH3Raw >= 0 && resH3Raw <= 15 ? resH3Raw : 9;
        const sortBy = (req.query.sort || 'mix').toString();

        const recs = await fetchLiveSensors({ lat, lng, radiusMeters, limit: 2000 });
        const items = recs.map(mapSensorRecordToParking);
        // Ensure unknown availability does not get treated as zero by downstream logic
        for (const it of items) {
          if (it.available_spots === null || it.available_spots === undefined) {
            it.available_spots = null;
          }
        }
        const areas = aggregateSensorsToAreas(items, { res: resH3 });
        const ranked = rankAreas(areas, { lat, lng, by: sortBy });

        const top = ranked.slice(0, limit);
        // record a sample for in-memory history
        for (const item of top) {
          recordAreaSample(item.area_id, {
            free: Number(item.available_bays || 0),
            occ: Math.max(0, Number(item.total_bays || 0) - Number(item.available_bays || 0)),
            total: Number(item.total_bays || 0),
            ts: item.updated_at ? new Date(item.updated_at) : new Date()
          });
        }
        res.json(top);
    } catch (err) {
        console.error('GET /parking/areas error:', err);
        res.status(500).json({ error: 'Failed to compute parking areas', detail: String(err.message || err) });
    }
});

// Convenience: history by coordinates (build H3 areaId from lat/lng + res)
app.get('/api/v1/parking/areas/bycoord/history', async (req, res) => {
    try {
        const lat = Number(req.query.lat);
        const lng = Number(req.query.lng);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const resolution = Number(req.query.res ?? 9);
        const source = String(req.query.source || 'live');
        const year = Number(req.query.year || process.env.ANNUAL_DEFAULT_YEAR || 2019);

        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
            return res.status(400).json({ error: 'Missing lat/lng' });
        }

        if (source === 'annual') {
            const yearStart = new Date(Date.UTC(year, 0, 1, 0, 0, 0)).toISOString();
            const yearEnd = (year === 2020 && String(process.env.ANNUAL_2020_MAY_ONLY || '1') === '1')
                ? new Date(Date.UTC(2020, 4, 31, 23, 59, 59)).toISOString()
                : new Date(Date.UTC(year, 11, 31, 23, 59, 59)).toISOString();

            const dsList = (process.env.ANNUAL_EVENTS_DATASETS || 'on-street-car-parking-sensor-data-2019,on-street-car-parking-sensor-data-2020-jan-may')
                .split(',').map(s => s.trim()).filter(Boolean);
            let datasetIds = dsList.filter(id => id.includes(String(year)));
            if (!datasetIds.length) datasetIds = dsList.length ? dsList : undefined;

            const ods = require('./lib/opendatasoft');
            const { capacity, series } = await ods.fetchAnnualHourlyByRadius({
                lat, lng, radiusMeters, startISO: yearStart, endISO: yearEnd, datasetIds
            });
            return res.json({ area_id: 'bycoord', res: resolution, year, total_bays: capacity, series, source: 'annual' });
        }

        // live/default fallback using EVENTS_DATASET_ID (if configured)
        const ods = require('./lib/opendatasoft');
        let rows = await ods.fetchEventsHourlyCounts({ days: Number(req.query.days || 30), lat, lng, radiusMeters });
        if (!Array.isArray(rows)) rows = [];
        const total = rows.length ? Math.max(...rows.map(r => r.total || 0)) : 0;
        return res.json({ area_id: 'bycoord', res: resolution, total_bays: total, series: rows, source: 'live' });
    } catch (err) {
        console.error('GET /parking/areas/bycoord/history error:', err);
        res.status(500).json({ error: 'Failed to load history', detail: String(err.message || err) });
    }
});
// Convenience: forecast by coordinates (build H3 areaId from lat/lng + res)
app.get('/api/v1/parking/areas/bycoord/forecast', async (req, res) => {
    try {
        const h3 = require('h3-js');
        const toCell = h3.latLngToCell || h3.geoToH3;
        const lat = Number(req.query.lat ?? -37.8136);
        const lng = Number(req.query.lng ?? 144.9631);
        const resRaw = Number(req.query.res ?? 9);
        const resH3 = Number.isInteger(resRaw) && resRaw >= 0 && resRaw <= 15 ? resRaw : 9;
        const areaId = toCell(lat, lng, resH3);

        const source = String(req.query.source || 'live');
        const hours  = Math.min(48, Math.max(1, Number(req.query.hours || 24)));
        const days   = Math.min(3650, Math.max(7, Number(req.query.days || 365)));
        const radiusMeters = Number(req.query.radius ?? 1200);

        let total = 0;
        let rows = [];

        if (source === 'annual') {
            const year = Number(req.query.year || process.env.ANNUAL_DEFAULT_YEAR || 2019);
            const yearStart = new Date(Date.UTC(year, 0, 1, 0, 0, 0)).toISOString();
            const yearEnd = (year === 2020 && String(process.env.ANNUAL_2020_MAY_ONLY || '1') === '1')
                ? new Date(Date.UTC(2020, 4, 31, 23, 59, 59)).toISOString()
                : new Date(Date.UTC(year, 11, 31, 23, 59, 59)).toISOString();
            const dsList = (process.env.ANNUAL_EVENTS_DATASETS || 'on-street-car-parking-sensor-data-2019,on-street-car-parking-sensor-data-2020-jan-may')
                .split(',').map(s => s.trim()).filter(Boolean);
            let datasetIds = dsList.filter(id => id.includes(String(year)));
            if (!datasetIds.length) datasetIds = dsList.length ? dsList : undefined;

            const ods = require('./lib/opendatasoft');
            const { capacity, series } = await ods.fetchAnnualHourlyByRadius({ lat, lng, radiusMeters, startISO: yearStart, endISO: yearEnd, datasetIds });
            total = Math.max(1, capacity);
            rows  = series;
        } else {
            // 走原有 live 模式的历史（如果有）
            const { members } = await resolveAreaMembers(areaId, { lat, lng, radiusMeters, resolution: resH3 });
            total = Math.max(1, members.length);
            const ods = require('./lib/opendatasoft');
            if (ods && typeof ods.fetchParkingEventsHourlyByIds === 'function') {
                rows = await ods.fetchParkingEventsHourlyByIds({ ids: members.map(m=>m.id), days });
            } else {
                rows = await fetchEventsHourlyCounts({ days, lat, lng, radiusMeters });
            }
        }

        // 预测：按“历史小时模式”（HOD）取均值 + 经验分位数
        // 若 rows 含 total 字段，则以历史里的最大 total 作为容量基准
        if (Array.isArray(rows) && rows.length) {
          const totalsInRows = rows.map(r => Number(r.total || 0)).filter(n => Number.isFinite(n) && n > 0);
          const totalFromRows = totalsInRows.length ? Math.max(...totalsInRows) : 0;
          if (totalFromRows > 0) total = Math.max(total, totalFromRows);
        }
        const { mean, q10, q90 } = buildHodProfile(rows);
        const out = forecastNextHoursFromHod({ hours, baseTotal: total, mean, q10, q90 });

        res.json({ area_id: areaId, res: resH3, total_bays: total, horizon_hours: hours, series: out, source });
    } catch (err) {
        console.error('GET /parking/areas/bycoord/forecast error:', err);
        res.status(500).json({ error: 'Failed to load forecast by coords', detail: String(err.message || err) });
    }
});

// GET /api/v1/parking/areas/:areaId — 区域汇总 + 成员车位（最多 200）
// GET /api/v1/parking/areas/:areaId — 区域汇总 + 成员车位（最多 200）
app.get('/api/v1/parking/areas/:areaId', async (req, res) => {
    try {
        const areaId = req.params.areaId;
        const lat = Number(req.query.lat ?? -37.8136);
        const lng = Number(req.query.lng ?? 144.9631);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const passedRes = Number(req.query.res);

        const { members, resolution: resolvedRes } = await resolveAreaMembers(areaId, { lat, lng, radiusMeters, resolution: passedRes });
        const safeRes = (Number.isInteger(resolvedRes) && resolvedRes >= 0 && resolvedRes <= 15) ? resolvedRes : 9;
        const areas = aggregateSensorsToAreas(members, { res: safeRes });
        if (areas.length === 0) return res.status(404).json({ error: 'Area not found or empty', resolution: safeRes });
        const summary = areas[0];
        summary.members = members.slice(0, 200);
        summary.resolution = safeRes;
        res.json(summary);
    } catch (err) {
        const msg = String(err && (err.message || err));
        if (msg.includes('Invalid H3 areaId')) {
            return res.status(400).json({ error: 'Invalid area_id', detail: msg });
        }
        console.error('GET /parking/areas/:areaId error:', err);
        res.status(500).json({ error: 'Failed to compute area', detail: msg });
    }
});



// GET /api/v1/parking?lat=-37.8136&lng=144.9631&radius=900&limit=500
// Optional: bbox=minLon,minLat,maxLon,maxLat (overrides lat/lng/radius)
app.get('/api/v1/parking', async (req, res) => {
  try {
    const { lat, lng, radius, limit, bbox } = req.query;

    const options = {};
    if (bbox) {
      const parts = String(bbox).split(',').map(Number);
      if (parts.length === 4 && parts.every(Number.isFinite)) {
        options.bbox = { minLon: parts[0], minLat: parts[1], maxLon: parts[2], maxLat: parts[3] };
      }
    } else {
      options.lat = toNumber(lat, -37.8136);
      options.lng = toNumber(lng, 144.9631);
      options.radiusMeters = toNumber(radius, 900);
    }
    options.limit = toNumber(limit, 100);

    const recs = await fetchLiveSensors(options);
    const items = recs.map(mapSensorRecordToParking);

    res.json(items);
  } catch (err) {
    console.error('GET /parking error:', err);
    res.status(500).json({ error: 'Failed to load parking data', detail: String(err.message || err) });
  }
});

// Best-effort detail lookup around a point
app.get('/api/v1/parking/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const lat = toNumber(req.query.lat, -37.8136);
    const lng = toNumber(req.query.lng, 144.9631);
    const radiusMeters = toNumber(req.query.radius, 1000);

    const recs = await fetchLiveSensors({ lat, lng, radiusMeters, limit: 1000 });
    const items = recs.map(mapSensorRecordToParking);
    const found = items.find(p => p.id === id) || items.find(p => (p.name || '').includes(id));
    if (!found) return res.status(404).json({ error: 'Not found' });
    res.json(found);
  } catch (err) {
    console.error('GET /parking/:id error:', err);
    res.status(500).json({ error: 'Failed', detail: String(err.message || err) });
  }
});

// Historical stats (V0): busiestHours (if events dataset configured) + averageOccupancy from live snapshot
app.get('/api/v1/stats/parking', async (req, res) => {
  try {
    const sinceDays = toNumber(req.query.days, 7);
    const lat = toNumber(req.query.lat, -37.8136);
    const lng = toNumber(req.query.lng, 144.9631);
    const radiusMeters = toNumber(req.query.radius, 1200);

    // Busy hours from ANNUAL datasets (preferred)
    let busiestHours = [];
    try {
      const ods = require('./lib/opendatasoft');
      if (ods && typeof ods.fetchAnnualHourlyByRadius === 'function') {
        const endISO   = new Date().toISOString();
        const startISO = new Date(Date.now() - sinceDays*24*3600*1000).toISOString();
        const { series } = await ods.fetchAnnualHourlyByRadius({ lat, lng, radiusMeters, startISO, endISO });
        // Map to simple {hour: ts, count: occ}
        busiestHours = series.map(s => ({ hour: s.ts, count: s.occ }));
      }
    } catch (e) {
      busiestHours = [];
    }

    // Coarse average occupancy from current snapshot (~100m grid, top 10)
    const recs = await fetchLiveSensors({ lat, lng, radiusMeters, limit: 2000 });
    const items = recs.map(mapSensorRecordToParking);
    const buckets = new Map();
    for (const p of items) {
      if (!Number.isFinite(p.lat) || !Number.isFinite(p.lng)) continue;
      const key = `${p.lat.toFixed(3)},${p.lng.toFixed(3)}`;
      const b = buckets.get(key) || { carPark: key, total: 0, occ: 0 };
      b.total += 1;
      b.occ += (p.available_spots ? 0 : 1);
      buckets.set(key, b);
    }
    const averageOccupancy = Array.from(buckets.values())
      .map(b => ({ carPark: b.carPark, percentage: Math.round((b.occ / Math.max(1,b.total))*100) }))
      .sort((a,b) => b.percentage - a.percentage)
      .slice(0, 10);

    res.json({ averageOccupancy, busiestHours });
  } catch (err) {
    console.error('GET /stats/parking error:', err);
    res.status(500).json({ error: 'Failed to compute stats', detail: String(err.message || err) });
  }
});



// GET /api/v1/parking/areas/:areaId/history — Historical hourly stats for an area (by areaId)
app.get('/api/v1/parking/areas/bycoord/history', async (req, res) => {
    try {
        const lat = Number(req.query.lat);
        const lng = Number(req.query.lng);
        const radiusMeters = Number(req.query.radius ?? 1200);
        const resolution = Number(req.query.res ?? 9);
        const source = String(req.query.source || 'live');
        const year = Number(req.query.year || process.env.ANNUAL_DEFAULT_YEAR || 2019);

        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
            return res.status(400).json({ error: 'Missing lat/lng' });
        }

        if (source === 'annual') {
            const yearStart = new Date(Date.UTC(year, 0, 1, 0, 0, 0)).toISOString();
            const yearEnd = (year === 2020 && String(process.env.ANNUAL_2020_MAY_ONLY || '1') === '1')
                ? new Date(Date.UTC(2020, 4, 31, 23, 59, 59)).toISOString()
                : new Date(Date.UTC(year, 11, 31, 23, 59, 59)).toISOString();

            const dsList = (process.env.ANNUAL_EVENTS_DATASETS || '')
                .split(',').map(s => s.trim()).filter(Boolean);
            let datasetIds = dsList.filter(id => id.includes(String(year)));
            if (!datasetIds.length) datasetIds = dsList.length ? dsList : undefined;

            const ods = require('./lib/opendatasoft');
            const { capacity, series } = await ods.fetchAnnualHourlyByRadius({
                lat, lng, radiusMeters, startISO: yearStart, endISO: yearEnd, datasetIds
            });
            return res.json({ area_id: 'bycoord', res: resolution, year, total_bays: capacity, series, source: 'annual' });
        }

        // live/default
        const ods = require('./lib/opendatasoft');
        let rows = await ods.fetchEventsHourlyCounts({ days: Number(req.query.days || 30), lat, lng, radiusMeters });
        if (!Array.isArray(rows)) rows = [];
        const total = rows.length ? Math.max(...rows.map(r => r.total || 0)) : 0;
        return res.json({ area_id: 'bycoord', res: resolution, total_bays: total, series: rows, source: 'live' });
    } catch (err) {
        console.error('GET /parking/areas/bycoord/history error:', err);
        res.status(500).json({ error: 'Failed to load history', detail: String(err.message || err) });
    }
});
// GET /api/v1/parking/areas/:areaId/history — Historical hourly stats for an area (by areaId)

// GET /api/v1/parking/areas/:areaId/forecast — Forecast for an area (by areaId)
app.get('/api/v1/parking/areas/:areaId/forecast', async (req, res) => {
  try {
    const areaId = req.params.areaId;
    const h3 = require('h3-js');
    const toLatLng = h3.cellToLatLng || h3.h3ToGeo;
    const [cLat, cLng] = toLatLng(areaId);

    const source = String(req.query.source || 'live');
    const lat = Number(req.query.lat ?? cLat);
    const lng = Number(req.query.lng ?? cLng);
    const radiusMeters = Number(req.query.radius ?? 1200);
    const passedRes = Number(req.query.res);
    const hours = Math.min(48, Math.max(1, Number(req.query.hours || 24)));
    const days = Math.min(3650, Math.max(7, Number(req.query.days || 365)));

    let total = 0;
    let rows = [];

    if (source === 'annual') {
      const year = Number(req.query.year || process.env.ANNUAL_DEFAULT_YEAR || 2019);
      const yearStart = new Date(Date.UTC(year, 0, 1, 0, 0, 0)).toISOString();
      const yearEnd = (year === 2020 && String(process.env.ANNUAL_2020_MAY_ONLY || '1') === '1')
        ? new Date(Date.UTC(2020, 4, 31, 23, 59, 59)).toISOString()
        : new Date(Date.UTC(year, 11, 31, 23, 59, 59)).toISOString();
      const dsList = (process.env.ANNUAL_EVENTS_DATASETS || 'on-street-car-parking-sensor-data-2019,on-street-car-parking-sensor-data-2020-jan-may')
        .split(',').map(s => s.trim()).filter(Boolean);
      let datasetIds = dsList.filter(id => id.includes(String(year)));
      if (!datasetIds.length) datasetIds = dsList.length ? dsList : undefined;

      const ods = require('./lib/opendatasoft');
      const { capacity, series } = await ods.fetchAnnualHourlyByRadius({
        lat: cLat, lng: cLng, radiusMeters, startISO: yearStart, endISO: yearEnd, datasetIds
      });
      total = Math.max(1, capacity);
      rows  = series;
      if (!Array.isArray(rows) || !rows.length) {
        const memSeries = readAreaSeries(areaId, days);
        if (Array.isArray(memSeries) && memSeries.length) rows = memSeries;
      }
    } else {
      const { members } = await resolveAreaMembers(areaId, { lat, lng, radiusMeters, resolution: passedRes });
      total = Math.max(1, members.length);
      const ods = require('./lib/opendatasoft');
      if (ods && typeof ods.fetchParkingEventsHourlyByIds === 'function') {
        rows = await ods.fetchParkingEventsHourlyByIds({ ids: members.map(m=>m.id), days });
      } else {
        rows = await fetchEventsHourlyCounts({ days, lat, lng, radiusMeters });
      }
      if (!Array.isArray(rows) || rows.length === 0) {
        const memSeries = readAreaSeries(areaId, days);
        rows = memSeries;
      }
    }

    // Forecast using hour-of-day profile (mean + empirical quantiles)
    if (Array.isArray(rows) && rows.length) {
      const totalsInRows = rows.map(r => Number(r.total || 0)).filter(n => Number.isFinite(n) && n > 0);
      const totalFromRows = totalsInRows.length ? Math.max(...totalsInRows) : 0;
      if (totalFromRows > 0) total = Math.max(total, totalFromRows);
    }
    const { mean, q10, q90 } = buildHodProfile(rows);
    const out = forecastNextHoursFromHod({ hours, baseTotal: total, mean, q10, q90 });

    res.json({ area_id: areaId, total_bays: total, horizon_hours: hours, series: out, source });
  } catch (err) {
    console.error('GET /parking/areas/:areaId/forecast error:', err);
    res.status(500).json({ error: 'Failed to load area forecast', detail: String(err.message || err) });
  }
});

// Debug: inspect in-memory area history
app.get('/api/v1/debug/area-history', (req, res) => {
  const areaId = String(req.query.areaId || '').trim();
  const days = Math.max(1, Number(req.query.days || 14));
  if (!areaId) return res.status(400).json({ error: 'Missing areaId' });
  const series = readAreaSeries(areaId, days);
  res.json({ area_id: areaId, days, points: series.length, series });
});
// Guards for insights globals (safe defaults)
if (typeof window !== 'undefined') {
  if (!('api' in window) || typeof window.api !== 'object') window.api = {};
  if (!('API_BASE' in window)) window.API_BASE = '/api/v1';
  if (!('USE_MOCK' in window)) window.USE_MOCK = false;
}
// ---- Melbourne Insights (mock, deterministic, no upstream) ----
const INSIGHTS_REGIONS = ['Melbourne', 'Port Phillip', 'Stonnington', 'Yarra', 'Docklands', 'Southbank'];
const BASE_CPH = [1.22, 1.35, 1.28, 1.10, 0.88, 0.95]; // cars per household baseline by region

function clamp(n, min, max) { return Math.max(min, Math.min(max, n)); }
function toInt(v, dflt) { const n = parseInt(String(v), 10); return Number.isFinite(n) ? n : dflt; }

// 计算各区“每户拥有车辆数”（与前端 mock 同步的公式，保证前后端一致）
function carOwnershipFor(year, region) {
    const jitter = (i) => Math.sin((year + i) * 0.7) * 0.05; // ±0.05 上下波动
    const rows = INSIGHTS_REGIONS.map((name, i) => ({
        region: name,
        cars_per_household: +clamp(BASE_CPH[i] + jitter(i), 0.6, 2.5).toFixed(2)
    }));
    if (region && region !== 'ALL') {
        const found = rows.find(r => r.region === region);
        return found ? [found] : [];
    }
    return rows;
}

// 生成从 2011 年起到 year 的 CBD 人口时间序列（与前端 mock 同步）
function cbdPopulationSeries(year) {
    const start = 2011;
    const end = Math.max(start, year);
    let p = 35000; // 起点
    const series = [];
    for (let y = start; y <= end; y++) {
        p = Math.round(p * (1 + (y % 7 === 0 ? 0.025 : 0.018)));
        if (y === 2020 || y === 2021) p = Math.round(p * 0.96); // 疫情年下探
        series.push({ year: y, population: p });
    }
    return series;
}

// GET /api/v1/insights/regions  → ['Melbourne', 'Port Phillip', ...]
app.get('/api/v1/insights/regions', (req, res) => {
    return res.json(INSIGHTS_REGIONS);
});

// GET /api/v1/insights/car-ownership?year=YYYY[&region=Melbourne]
// - 不带 region → 返回数组 [{region, cars_per_household}, ...]
// - 带 region → 返回对象 {region, cars_per_household}（前端已兼容两种返回）
app.get('/api/v1/insights/car-ownership', (req, res) => {
    const year = toInt(req.query.year, new Date().getFullYear() - 1);
    const region = typeof req.query.region === 'string' ? req.query.region : null;
    const data = carOwnershipFor(year, region);

    if (region && region !== 'ALL') {
        if (!data.length) return res.status(404).json({ error: 'Region not found', region });
        return res.json(data[0]);
    }
    return res.json(data);
});

// GET /api/v1/insights/cbd-population?year=YYYY
// - 返回 { series: [{year, population}, ...] }
app.get('/api/v1/insights/cbd-population', (req, res) => {
    const year = toInt(req.query.year, new Date().getFullYear() - 1);
    const series = cbdPopulationSeries(year);
    return res.json({ series });
});
app.listen(PORT, () => console.log(`Backend running on http://localhost:${PORT}`));

// Fallback: if not booted above for some reason, attach once
if (typeof document !== 'undefined' && !document.__insightsBootBound) {
  document.__insightsBootBound = true;
  document.addEventListener('DOMContentLoaded', () => {
    if (typeof initInsights === 'function') initInsights();
  });
}