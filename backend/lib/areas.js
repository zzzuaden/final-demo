// backend/lib/areas.js
const h3 = require('h3-js');
// 兼容 h3-js v4/v3
const latLngToCell   = h3.latLngToCell   || h3.geoToH3;
const cellToLatLng   = h3.cellToLatLng   || h3.h3ToGeo;
const cellToBoundary = h3.cellToBoundary || h3.h3ToGeoBoundary;
const getResolution  = h3.getResolution  || h3.h3GetResolution;

const { haversineMeters } = require('./geo');

/**
 * 把车位聚合到 H3 区域（六边形）
 * @param {Array} items - [{id, lat, lng, capacity, available_spots, updated_at}]
 * @param {Object} opts
 * @param {number} opts.res - H3 分辨率（默认 9：街区级）
 */
function aggregateSensorsToAreas(items, { res = 9 } = {}){
    const buckets = new Map();

    for (const p of items){
        if (!p || !Number.isFinite(p.lat) || !Number.isFinite(p.lng)) continue;
        const cell = latLngToCell(p.lat, p.lng, res);
        const b = buckets.get(cell) || {
            area_id: cell,
            total_bays: 0,
            available_bays: 0,
            updated_at: null,
            sample_bays: [],
        };
        const cap = Number.isFinite(p.capacity) ? p.capacity : 1;
        const avail = Number(p.available_spots) || 0;

        b.total_bays += cap;
        b.available_bays += avail;
        if (b.sample_bays.length < 10 && p.id) b.sample_bays.push(p.id);

        const curTs = new Date(p.updated_at || Date.now()).getTime();
        const maxTs = b.updated_at ? new Date(b.updated_at).getTime() : 0;
        if (curTs > maxTs) b.updated_at = (new Date(curTs)).toISOString();

        buckets.set(cell, b);
    }

    const areas = [];
    for (const b of buckets.values()){
        const center = cellToLatLng(b.area_id); // [lat, lng]
        const boundary = cellToBoundary(b.area_id, true); // [[lat,lng],...]
        const occRate = b.total_bays > 0 ? 1 - (b.available_bays / b.total_bays) : 0;
        areas.push({
            area_id: b.area_id,
            center: { lat: center[0], lng: center[1] },
            boundary: boundary.map(([lat, lng]) => [lat, lng]),
            total_bays: b.total_bays,
            available_bays: b.available_bays,
            occupancy_rate: Number(occRate.toFixed(2)),
            updated_at: b.updated_at,
            sample_bays: b.sample_bays
        });
    }

    return areas;
}

/**
 * 对区域做“附近推荐”打分
 * by: 'availability' | 'occupancy' | 'distance' | 'mix'
 */
function rankAreas(areas, { lat, lng, by='mix' } = {}){
    return areas.map(a => {
        const d = (Number.isFinite(lat) && Number.isFinite(lng)) ? haversineMeters(lat, lng, a.center.lat, a.center.lng) : null;
        const available = a.available_bays;
        const occ = a.occupancy_rate;
        let score = 0;
        if (by === 'availability') score = (available) - (d ? d/50 : 0);
        else if (by === 'occupancy') score = (1 - occ) - (d ? d/5000 : 0);
        else if (by === 'distance') score = -(d || 0);
        else { // mix
            score = (available * 2) - (d ? d/30 : 0) - (occ*10);
        }
        return { ...a, _distance_m: d, _score: Number(score.toFixed(3)) };
    }).sort((x,y) => y._score - x._score);
}

module.exports = { aggregateSensorsToAreas, rankAreas };