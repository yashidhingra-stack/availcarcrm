/* ==========================================================================
   Avail Car CRM - shared database (Firebase Firestore)
   Makes every user on every device see the same files, moves and Hierarchy
   settings. Fill in firebaseConfig below (see the setup steps), then upload
   this file next to index.html. While the config still says PASTE_..., the
   CRM keeps working exactly as before, saving in each browser only.
   ========================================================================== */
const firebaseConfig = {
  apiKey: "AIzaSyAAFHXTlrPTfnmK8GZraB6haIJDm1gO-Eg",
  authDomain: "crm-avail-car.firebaseapp.com",
  projectId: "crm-avail-car",
  storageBucket: "crm-avail-car.firebasestorage.app",
  messagingSenderId: "529897390682",
  appId: "1:529897390682:web:a1313510078cc65459ce63"
};

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { initializeFirestore, persistentLocalCache, persistentMultipleTabManager, collection, doc, onSnapshot, writeBatch, runTransaction, setDoc, getDocs, serverTimestamp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import { getAuth, signInAnonymously } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";

const W = window;
const CLIENT = Math.random().toString(36).slice(2, 10);
const SYNC_VERSION = "sync v3";
const status = (t) => { try { if (W.CRM_SYNC_STATUS) W.CRM_SYNC_STATUS(SYNC_VERSION + " - " + t); } catch (e) {} };

async function main() {
  if (!firebaseConfig.apiKey || /PASTE/.test(firebaseConfig.apiKey)) { status("Shared sync is off (not set up)"); return; }
  status("Connecting...");
  const app = initializeApp(firebaseConfig, "crm-sync");
  try { await signInAnonymously(getAuth(app)); } catch (e) { console.warn("CRM sync: anonymous sign-in unavailable, continuing without it", e); }
  let db;
  try { db = initializeFirestore(app, { localCache: persistentLocalCache({ tabManager: persistentMultipleTabManager() }) }); }
  catch (e) { db = initializeFirestore(app, {}); }

  const leadsCol = collection(db, "crm_leads");
  const metaRef = doc(db, "crm_config", "meta");
  const hierRef = doc(db, "crm_config", "hierarchy");
  const known = new Map();   /* lead id -> JSON text last seen in the cloud (or last sent) */
  let ready = false, migrating = false, latest = null, timer = null;
  const api = { allowBigDelete: false };

  const chunks = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; };
  const put = (batch, id, j) => batch.set(doc(leadsCol, id), { id: id, j: j, by: CLIENT, t: serverTimestamp() });

  async function uploadAll(list) {
    for (const part of chunks(list, 400)) {
      const b = writeBatch(db);
      part.forEach((l) => { const id = String(l.id), j = JSON.stringify(l); put(b, id, j); known.set(id, j); });
      await b.commit();
    }
  }

  function join(snap) {
    const arr = [];
    known.clear();
    snap.forEach((d) => { const j = d.data().j; known.set(d.id, j); try { arr.push(JSON.parse(j)); } catch (e) {} });
    W.CRM_APPLY_LEADS(arr, [], true);
    ready = true;
    status("Shared sync on - " + arr.length + " files");
  }

  async function claimAndUpload() {
    if (migrating) return;
    migrating = true;
    try {
      const mine = await runTransaction(db, async (tx) => {
        const m = await tx.get(metaRef);
        if (m.exists()) return (m.data() || {}).state || "other";
        tx.set(metaRef, { state: "uploading", by: CLIENT, t: serverTimestamp() });
        return "mine";
      });
      if (mine === "mine") {
        status("Uploading files to the shared database...");
        const list = W.CRM_GET_LEADS ? W.CRM_GET_LEADS() : [];
        await uploadAll(list);
        await setDoc(metaRef, { state: "done", count: list.length }, { merge: true });
        ready = true;
        status("Shared sync on - uploaded " + list.length + " files");
      } else if (mine === "done") {
        /* The shared database was set up before and is now EMPTY (Admin cleared it): empty is the truth.
           Never upload this device's old browser copy back into it - that is what made old files return. */
        W.CRM_APPLY_LEADS([], [], true);
        ready = true; migrating = false;
        status("Shared sync on - 0 files");
      } else {
        status("Waiting for the first upload from another device...");
        migrating = false;
      }
    } catch (e) { console.warn("CRM sync upload failed", e); status("Sync error - check Firebase rules"); migrating = false; }
  }

  onSnapshot(leadsCol, { includeMetadataChanges: true }, (snap) => {
    if (!ready) {
      if (migrating) return; /* this device is uploading: ignore its own partial snapshots */
      if (snap.metadata.fromCache) { status("Connecting..."); return; }
      if (snap.empty) { claimAndUpload(); return; }
      join(snap);
      return;
    }
    const up = [], rem = [];
    snap.docChanges().forEach((ch) => {
      const id = ch.doc.id;
      if (ch.type === "removed") { if (known.has(id)) { known.delete(id); rem.push(id); } return; }
      const j = ch.doc.data().j;
      if (known.get(id) !== j) { known.set(id, j); try { up.push(JSON.parse(j)); } catch (e) {} }
    });
    if (up.length || rem.length) W.CRM_APPLY_LEADS(up, rem, false);
  }, (err) => { console.warn("CRM sync listener error", err); status("Sync error - check Firebase rules"); });

  async function flush() {
    const list = latest; if (!list || !ready) return;
    const seen = new Set(), writes = [], dels = [];
    for (const l of list) { const id = String(l.id); seen.add(id); const j = JSON.stringify(l); if (known.get(id) !== j) writes.push([id, j]); }
    known.forEach((_, id) => { if (!seen.has(id)) dels.push(id); });
    if (dels.length > Math.max(50, known.size * 0.2) && !api.allowBigDelete) {
      console.warn("CRM sync paused: this change would delete " + dels.length + " shared files. Reload the page to restore them.");
      status("Sync paused (too many deletions) - reload the page");
      return;
    }
    api.allowBigDelete = false; /* one-time permission, used by "Clear all leads" */
    const ops = writes.map((w) => ["s", w[0], w[1], known.get(w[0])]).concat(dels.map((id) => ["d", id, null, known.get(id)]));
    ops.forEach((o) => { if (o[0] === "s") known.set(o[1], o[2]); else known.delete(o[1]); });
    for (const part of chunks(ops, 400)) {
      const b = writeBatch(db);
      part.forEach((o) => { if (o[0] === "s") put(b, o[1], o[2]); else b.delete(doc(leadsCol, o[1])); });
      b.commit().then(() => status("Shared sync on")).catch((e) => {
        console.warn("CRM sync write failed", e);
        part.forEach((o) => { if (o[3] === undefined) known.delete(o[1]); else known.set(o[1], o[3]); });
        status("Sync error - will retry on the next change");
      });
    }
  }
  /* Delete EVERY lead document from the shared database (asks the server, not this browser's memory) */
  api.wipeCloud = async () => {
    clearTimeout(timer); latest = null;
    status("Clearing the shared database...");
    const snap = await getDocs(leadsCol);
    for (const part of chunks(snap.docs, 400)) {
      const b = writeBatch(db);
      part.forEach((d) => b.delete(d.ref));
      await b.commit();
    }
    known.clear();
    await setDoc(metaRef, { state: "done", count: 0, wipedBy: CLIENT, wipedAt: serverTimestamp() }, { merge: true });
    ready = true; migrating = false;
    status("Shared sync on - 0 files (cleared " + snap.size + ")");
    return snap.size;
  };
  api.pushLeads = (list) => { if (!ready) return; latest = list; clearTimeout(timer); timer = setTimeout(flush, 500); };

  /* Hierarchy and category access, shared the same way */
  let lastHier = null;
  onSnapshot(hierRef, { includeMetadataChanges: true }, (snap) => {
    if (snap.exists()) {
      const j = snap.data().j;
      if (j !== lastHier) { lastHier = j; try { W.CRM_APPLY_HIER(JSON.parse(j)); } catch (e) {} }
    } else if (!snap.metadata.fromCache && lastHier === null) {
      const cur = W.CRM_GET_HIER ? W.CRM_GET_HIER() : null;
      if (cur) { lastHier = JSON.stringify(cur); setDoc(hierRef, { j: lastHier, by: CLIENT, t: serverTimestamp() }).catch(() => { lastHier = null; }); }
    }
  });
  api.pushHier = (h) => {
    const j = JSON.stringify(h); if (j === lastHier) return;
    lastHier = j;
    setDoc(hierRef, { j: j, by: CLIENT, t: serverTimestamp() }).catch(() => { lastHier = null; });
  };
  Object.defineProperty(api, "ready", { get: () => ready });
  W.CRM_SYNC = api;
}

main().catch((e) => { console.warn("CRM sync failed to start", e); status("Shared sync unavailable - working on this device only"); });
