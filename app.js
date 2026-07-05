import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason } from '@whiskeysockets/baileys';
import pino from 'pino';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import QRCode from 'qrcode';
import fs from 'fs';
import path from 'path';
import cors from 'cors';
import { fileURLToPath } from 'url';
import { exec, spawn, execSync } from 'child_process';
import { promisify } from 'util';
import ffmpegPath from 'ffmpeg-static';
import { createProxyMiddleware } from 'http-proxy-middleware';
import os from 'os';

const execPromise = promisify(exec);

// Session backup/restore helpers
// Saves auth_info_baileys as base64 JSON to a backup file so sessions survive Railway restarts
const SESSION_BACKUP_PATH = process.env.SESSION_BACKUP_PATH || path.join(os.tmpdir(), 'wa_session_backup.json');

function backupSession(authDir) {
    try {
        if (!fs.existsSync(authDir)) return;
        const files = {};
        fs.readdirSync(authDir).forEach(f => {
            files[f] = fs.readFileSync(path.join(authDir, f), 'base64');
        });
        fs.writeFileSync(SESSION_BACKUP_PATH, JSON.stringify(files));
    } catch(e) { console.error('Session backup failed:', e.message); }
}

function restoreSession(authDir) {
    try {
        if (!fs.existsSync(SESSION_BACKUP_PATH)) return false;
        if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });
        const files = JSON.parse(fs.readFileSync(SESSION_BACKUP_PATH, 'utf8'));
        Object.entries(files).forEach(([name, b64]) => {
            fs.writeFileSync(path.join(authDir, name), Buffer.from(b64, 'base64'));
        });
        console.log('[Session] Restored session from backup.');
        return true;
    } catch(e) { console.error('Session restore failed:', e.message); return false; }
}

// Resolve __dirname in ES module context
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Helper function to transcode audio to strict OGG/Opus format using FFmpeg and return the file path
async function convertToOggOpusFile(base64Data) {
    const tempDir = path.join(__dirname, 'temp_audio');
    if (!fs.existsSync(tempDir)) {
        fs.mkdirSync(tempDir, { recursive: true });
    }
    
    const randomId = Math.random().toString(36).substring(7);
    const inputPath = path.join(tempDir, `input_${randomId}.webm`);
    const outputPath = path.join(tempDir, `output_${randomId}.ogg`);
    
    // Ensure executable permissions on non-Windows
    if (process.platform !== 'win32') {
        try {
            fs.chmodSync(ffmpegPath, 0o755);
        } catch (e) {
            console.error('Failed to chmod ffmpeg-static:', e);
        }
    }
    
    try {
        const buffer = Buffer.from(base64Data.split(',')[1] || base64Data, 'base64');
        fs.writeFileSync(inputPath, buffer);
        
        // Transcode WebM/MP4 audio to Mono, 48kHz, Opus codec OGG container for WhatsApp compatibility
        // Try libopus first
        let command = `"${ffmpegPath}" -y -i "${inputPath}" -vn -c:a libopus -b:a 48k -ac 1 -avoid_negative_ts make_zero -f ogg "${outputPath}"`;
        try {
            await execPromise(command);
        } catch (err) {
            addLog(`[FFmpeg libopus failed, trying native opus] ${err.message}`);
            // Fallback to native opus encoder
            command = `"${ffmpegPath}" -y -i "${inputPath}" -vn -c:a opus -b:a 48k -ac 1 -avoid_negative_ts make_zero -f ogg "${outputPath}"`;
            await execPromise(command);
        }
        
        if (fs.existsSync(inputPath)) {
            try { fs.unlinkSync(inputPath); } catch (e) {}
        }
        
        return { filePath: outputPath, isTranscoded: true };
    } catch (err) {
        addLog(`[FFmpeg transcoding failed completely] ${err.message}`);
        // Fallback: write raw webm to temporary file and return it
        const fallbackPath = path.join(tempDir, `fallback_${randomId}.webm`);
        try {
            const buffer = Buffer.from(base64Data.split(',')[1] || base64Data, 'base64');
            fs.writeFileSync(fallbackPath, buffer);
        } catch (writeErr) {
            addLog(`[FFmpeg fallback write failed] ${writeErr.message}`);
        }
        
        // Cleanup input and output
        if (fs.existsSync(inputPath)) { try { fs.unlinkSync(inputPath); } catch (e) {} }
        if (fs.existsSync(outputPath)) { try { fs.unlinkSync(outputPath); } catch (e) {} }
        
        return { filePath: fallbackPath, isTranscoded: false };
    }
}

// Helper to match keywords as whole words or exact phrases (preventing loose substring matching)
function matchKeyword(messageText, keyword) {
    const msg = messageText.trim().toLowerCase();
    const kw = keyword.trim().toLowerCase();
    
    if (msg === kw) return true;
    
    let pattern = '';
    if (/^\w/.test(kw)) pattern += '\\b';
    pattern += kw.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
    if (/\w$/.test(kw)) pattern += '\\b';
    
    try {
        const regex = new RegExp(pattern, 'i');
        return regex.test(msg);
    } catch (e) {
        return msg.includes(kw);
    }
}

// Initialize logs container
const logs = [];
function addLog(text) {
    const time = new Date().toLocaleTimeString();
    const logItem = { time, text };
    logs.push(logItem);
    if (logs.length > 200) logs.shift();
    if (io) {
        io.emit('log', logItem);
    }
    console.log(`[${time}] ${text}`);
}

// Keyword handlers
// Allow overriding the keywords file path via env var (useful for Railway persistent volumes)
const isRailway = !!(process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY_SERVICE_ID);

// Detect persistent volume directory (e.g. /data on Railway)
let persistentDir = process.env.PERSISTENT_DIR;
if (!persistentDir && fs.existsSync('/data')) {
    persistentDir = '/data';
}

const AUTH_DIR = process.env.AUTH_DIR || (persistentDir ? path.join(persistentDir, 'auth_info_baileys') : (isRailway ? path.join(__dirname, 'auth_info_baileys') : path.join(__dirname, '..', 'auth_info_baileys')));

// Ensure persistent storage directory exists
if (!fs.existsSync(AUTH_DIR)) {
    try {
        fs.mkdirSync(AUTH_DIR, { recursive: true });
    } catch (e) {
        console.error('Failed to create AUTH_DIR:', e);
    }
}

const KEYWORDS_PATH = process.env.KEYWORDS_PATH || (persistentDir ? path.join(persistentDir, 'keywords.json') : (isRailway ? path.join(__dirname, 'keywords.json') : path.join(__dirname, '..', 'keywords.json')));
const CONTACTS_FILE = process.env.CONTACTS_FILE || (persistentDir ? path.join(persistentDir, 'active_contacts.json') : (isRailway ? path.join(__dirname, 'active_contacts.json') : path.join(__dirname, '..', 'active_contacts.json')));

// Initialize persistent keywords file if not present, copying from default template
const DEFAULT_KEYWORDS_PATH = path.join(__dirname, 'keywords.json');
if (!fs.existsSync(KEYWORDS_PATH)) {
    try {
        if (KEYWORDS_PATH !== DEFAULT_KEYWORDS_PATH && fs.existsSync(DEFAULT_KEYWORDS_PATH)) {
            fs.copyFileSync(DEFAULT_KEYWORDS_PATH, KEYWORDS_PATH);
            console.log('[Setup] Copied default keywords template to persistent storage.');
        } else {
            fs.writeFileSync(KEYWORDS_PATH, '{}', 'utf8');
        }
    } catch (e) {
        console.error('Failed to initialize persistent keywords.json:', e);
    }
}

// Ensure persistent contacts file exists
if (!fs.existsSync(CONTACTS_FILE)) {
    try {
        fs.writeFileSync(CONTACTS_FILE, '[]', 'utf8');
    } catch (e) {
        console.error('Failed to initialize active_contacts.json:', e);
    }
}

function loadKeywords() {
    try {
        if (fs.existsSync(KEYWORDS_PATH)) {
            return JSON.parse(fs.readFileSync(KEYWORDS_PATH, 'utf8'));
        }
    } catch (e) {
        addLog(`Error loading keywords: ${e.message}`);
    }
    return {};
}

function saveKeywords(kwMap) {
    try {
        fs.writeFileSync(KEYWORDS_PATH, JSON.stringify(kwMap, null, 2), 'utf8');
        return true;
    } catch (e) {
        addLog(`Error saving keywords: ${e.message}`);
        return false;
    }
}

function loadActiveContacts() {
    try {
        if (fs.existsSync(CONTACTS_FILE)) {
            const list = JSON.parse(fs.readFileSync(CONTACTS_FILE, 'utf8'));
            if (Array.isArray(list)) return list;
        }
    } catch (e) {
        console.error('Error loading active contacts:', e.message);
    }
    return [];
}

function saveActiveContacts(contacts) {
    try {
        fs.writeFileSync(CONTACTS_FILE, JSON.stringify(contacts, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error('Error saving active contacts:', e.message);
        return false;
    }
}

function addContact(jid) {
    if (!jid || !jid.endsWith('@s.whatsapp.net')) return;
    const contacts = loadActiveContacts();
    if (!contacts.includes(jid)) {
        contacts.push(jid);
        saveActiveContacts(contacts);
        addLog(`[Contact Sync] Added new active contact: ${jid}`);
    }
}

// Clears Baileys credentials and token state files, keeping rules and contacts intact
function clearSessionFiles(dir) {
    if (!fs.existsSync(dir)) return;
    try {
        fs.readdirSync(dir).forEach(f => {
            if (f !== 'keywords.json' && f !== 'active_contacts.json') {
                const filePath = path.join(dir, f);
                try {
                    const stat = fs.statSync(filePath);
                    if (stat.isDirectory()) {
                        fs.rmSync(filePath, { recursive: true, force: true });
                    } else {
                        fs.unlinkSync(filePath);
                    }
                } catch (e) {
                    console.error(`Failed to delete session file ${f}:`, e.message);
                }
            }
        });
        addLog('Session credentials cleared (rules & active contacts preserved).');
    } catch (e) {
        addLog(`Error during session cleanup: ${e.message}`);
    }
}

// ── YT Bot subprocess launcher ──────────────────────────────────────────────
// The Python Flask YT bot runs on internal port 8080, proxied at /yt/*
const PORT = process.env.PORT || 3000;
const YT_BOT_DIR = path.join(__dirname, 'yt-bot');
const YT_BOT_PORT = parseInt(PORT) + 1;
let ytBotProcess = null;

const FB_BOT_DIR = path.join(__dirname, 'fb-bot');
const FB_BOT_PORT = parseInt(PORT) + 2;
let fbBotProcess = null;

function findPythonBinary() {
    if (process.platform === 'win32') return 'python';
    
    // Dynamically append Nix profile bins to PATH so subprocesses can resolve Nix packages
    const nixBins = [
        '/root/.nix-profile/bin',
        '/home/nixpacks/.nix-profile/bin',
        '/nix/var/nix/profiles/default/bin'
    ];
    process.env.PATH = `${process.env.PATH}:${nixBins.join(':')}`;
    addLog(`[YT Debug] Extended PATH: ${process.env.PATH}`);

    try {
        const binSearch = execSync('which -a python python3 python3.11 python3.10 python3.9 python3.8 python3.7 2>&1 || true').toString().trim();
        addLog(`[YT Debug] 'which' search results:\n${binSearch}`);
    } catch (e) {}
    
    try {
        const binList = execSync('ls -la /usr/bin/python* /usr/local/bin/python* /root/.nix-profile/bin/python* /home/nixpacks/.nix-profile/bin/python* 2>/dev/null || true').toString().trim();
        if (binList) addLog(`[YT Debug] ls -la output:\n${binList}`);
    } catch (e) {}

    const candidates = ['python3', 'python', 'python3.11', 'python3.10', 'python3.9'];
    for (const name of candidates) {
        try {
            const binPath = execSync(`which ${name}`, { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim();
            if (binPath && fs.existsSync(binPath)) return binPath;
        } catch (e) {}
    }
    
    const commonPaths = [
        '/usr/bin/python3',
        '/usr/bin/python',
        '/usr/local/bin/python3',
        '/usr/local/bin/python',
        '/root/.nix-profile/bin/python3',
        '/root/.nix-profile/bin/python',
        '/home/nixpacks/.nix-profile/bin/python3',
        '/home/nixpacks/.nix-profile/bin/python'
    ];
    for (const p of commonPaths) {
        if (fs.existsSync(p)) return p;
    }
    return 'python3';
}

function startYTBot() {
    const ytAppPath = path.join(YT_BOT_DIR, 'app.py');
    if (!fs.existsSync(ytAppPath)) {
        console.log('[YT Bot] yt-bot/app.py not found — skipping YT bot startup.');
        return;
    }

    const pythonBin = findPythonBinary();
    console.log(`[YT Bot] Using python executable: ${pythonBin}`);
    addLog(`[YT Bot] Found python binary: ${pythonBin}`);
    console.log('[YT Bot] Starting Python Flask YT bot on internal port', YT_BOT_PORT);
    
    const ytProc = spawn(pythonBin, ['app.py'], {
        stdio: 'pipe',
        cwd: YT_BOT_DIR,
        env: { ...process.env, FLASK_PORT: String(YT_BOT_PORT) }
    });
    
    let ytErrorBuffer = [];
    
    ytProc.stdout.on('data', d => {
        const text = d.toString().trim();
        if (text) {
            console.log(`[YT Bot] ${text}`);
            if (text.includes('[LAUNCH]') || text.includes('Running')) {
                addLog(`[YT Bot] ${text}`);
            }
        }
    });
    
    ytProc.stderr.on('data', d => {
        const text = d.toString().trim();
        if (text) {
            console.error(`[YT Bot] ${text}`);
            ytErrorBuffer.push(text);
            if (ytErrorBuffer.length > 30) ytErrorBuffer.shift();
        }
    });
    
    ytProc.on('close', code => {
        addLog(`❌ [YT Bot] Subprocess exited with code ${code}.`);
        if (ytErrorBuffer.length > 0) {
            addLog(`❌ [YT Bot] Crash logs:`);
            ytErrorBuffer.forEach(line => addLog(`   👉 ${line}`));
        }
        addLog(`[YT Bot] Restarting in 5 seconds...`);
        ytBotProcess = null;
        setTimeout(startYTBot, 5000);
    });
    
    ytProc.on('error', err => {
        addLog(`❌ [YT Bot] Spawn error: ${err.message}`);
    });
    
    ytBotProcess = ytProc;
}

function startFBBot() {
    const fbAppPath = path.join(FB_BOT_DIR, 'app.py');
    if (!fs.existsSync(fbAppPath)) {
        console.log('[FB Bot] fb-bot/app.py not found — skipping FB bot startup.');
        return;
    }

    const pythonBin = findPythonBinary();
    console.log(`[FB Bot] Using python executable: ${pythonBin}`);
    addLog(`[FB Bot] Found python binary: ${pythonBin}`);
    console.log('[FB Bot] Starting Python Flask FB bot on internal port', FB_BOT_PORT);
    
    const fbProc = spawn(pythonBin, ['app.py'], {
        stdio: 'pipe',
        cwd: FB_BOT_DIR,
        env: { ...process.env, FLASK_PORT: String(FB_BOT_PORT) }
    });
    
    let fbErrorBuffer = [];
    
    fbProc.stdout.on('data', d => {
        const text = d.toString().trim();
        if (text) {
            console.log(`[FB Bot] ${text}`);
            if (text.includes('[LAUNCH]') || text.includes('Running')) {
                addLog(`[FB Bot] ${text}`);
            }
        }
    });
    
    fbProc.stderr.on('data', d => {
        const text = d.toString().trim();
        if (text) {
            console.error(`[FB Bot] ${text}`);
            fbErrorBuffer.push(text);
            if (fbErrorBuffer.length > 30) fbErrorBuffer.shift();
        }
    });
    
    fbProc.on('close', code => {
        addLog(`❌ [FB Bot] Subprocess exited with code ${code}.`);
        if (fbErrorBuffer.length > 0) {
            addLog(`❌ [FB Bot] Crash logs:`);
            fbErrorBuffer.forEach(line => addLog(`   👉 ${line}`));
        }
        addLog(`[FB Bot] Restarting in 5 seconds...`);
        fbBotProcess = null;
        setTimeout(startFBBot, 5000);
    });
    
    fbProc.on('error', err => {
        addLog(`❌ [FB Bot] Spawn error: ${err.message}`);
    });
    
    fbBotProcess = fbProc;
}

// Server setup
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

app.use(cors());

app.use('/yt', createProxyMiddleware({
    target: `http://127.0.0.1:${YT_BOT_PORT}`,
    changeOrigin: true,
    pathRewrite: { '^/yt': '' },
    on: {
        error: (err, req, res) => {
            console.error('[YT Proxy] Error:', err.message);
            if (!res.headersSent) {
                res.status(502).send('<html><body style="background:#07090f;color:#ef4444;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><div style="text-align:center"><div style="font-size:2rem">⚠️</div><div style="margin-top:1rem;font-size:1rem">YT Bot is starting up...<br><small style="color:#64748b">Refresh in a few seconds</small></div></div></body></html>');
            }
        }
    }
}));

app.use('/fb', createProxyMiddleware({
    target: `http://127.0.0.1:${FB_BOT_PORT}`,
    changeOrigin: true,
    on: {
        error: (err, req, res) => {
            console.error('[FB Proxy] Error:', err.message);
            if (!res.headersSent) {
                res.status(502).send('<html><body style="background:#07090f;color:#ef4444;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><div style="text-align:center"><div style="font-size:2rem">⚠️</div><div style="margin-top:1rem;font-size:1rem">FB Bot is starting up...<br><small style="color:#64748b">Refresh in a few seconds</small></div></div></body></html>');
            }
        }
    }
}));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// ── Proxy /yt/* → Python Flask YT bot ───────────────────────────────────────


app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/status', (req, res) => {
    res.json({
        status: connectionStatus,
        qr: qrCodeBase64
    });
});

app.get('/api/keywords', (req, res) => {
    res.json(loadKeywords());
});

app.post('/api/keywords', (req, res) => {
    const kwMap = req.body;
    if (saveKeywords(kwMap)) {
        res.json({ success: true, message: 'Keywords updated successfully.' });
        io.emit('keywords', kwMap);
    } else {
        res.status(500).json({ success: false, message: 'Failed to save keywords.' });
    }
});

app.post('/api/keywords/save-rule', (req, res) => {
    const { key, rule } = req.body;
    if (!key) {
        return res.status(400).json({ success: false, message: 'Keyword trigger is required.' });
    }
    const kwMap = loadKeywords();
    kwMap[key] = rule;
    if (saveKeywords(kwMap)) {
        res.json({ success: true, message: 'Rule saved successfully.' });
        io.emit('keywords', kwMap);
    } else {
        res.status(500).json({ success: false, message: 'Failed to save rule.' });
    }
});

app.post('/api/keywords/delete-rule', (req, res) => {
    const { key } = req.body;
    if (!key) {
        return res.status(400).json({ success: false, message: 'Keyword trigger is required.' });
    }
    const kwMap = loadKeywords();
    delete kwMap[key];
    if (saveKeywords(kwMap)) {
        res.json({ success: true, message: 'Rule deleted successfully.' });
        io.emit('keywords', kwMap);
    } else {
        res.status(500).json({ success: false, message: 'Failed to delete rule.' });
    }
});

app.get('/api/logs', (req, res) => {
    res.json(logs);
});

app.get('/api/debug-ffmpeg', async (req, res) => {
    try {
        let exists = fs.existsSync(ffmpegPath);
        let stats = exists ? fs.statSync(ffmpegPath) : null;
        
        if (exists && process.platform !== 'win32') {
            try {
                fs.chmodSync(ffmpegPath, 0o755);
            } catch(e) {
                addLog(`chmod in debug route failed: ${e.message}`);
            }
        }

        const { stdout, stderr } = await execPromise(`"${ffmpegPath}" -version`);
        res.json({
            success: true,
            exists,
            stats,
            ffmpegPath,
            stdout,
            stderr
        });
    } catch (err) {
        res.json({
            success: false,
            exists: fs.existsSync(ffmpegPath),
            ffmpegPath,
            error: err.message,
            stack: err.stack
        });
    }
});


app.post('/api/logout', async (req, res) => {
    try {
        if (sock) {
            addLog('Logging out of WhatsApp and clearing session...');
            await sock.logout();
            sock = null;
        }
        // Clear auth folder
        const authStateDir = AUTH_DIR;
        if (fs.existsSync(authStateDir)) {
            fs.rmSync(authStateDir, { recursive: true, force: true });
        }
        if (fs.existsSync(SESSION_BACKUP_PATH)) {
            fs.unlinkSync(SESSION_BACKUP_PATH);
        }
        connectionStatus = 'Disconnected';
        isConnecting = false;
        qrCodeBase64 = null;
        io.emit('status', { status: connectionStatus });
        io.emit('qr', { qr: null });
        addLog('Session cleared. Reconnecting for fresh QR scan...');
        setTimeout(() => connectToWhatsApp(), 1500);
        res.json({ success: true, message: 'Logged out. Scan the new QR code.' });
    } catch (err) {
        addLog(`Logout error: ${err.message}`);
        // Force clear even if logout() fails
        const authStateDir = AUTH_DIR;
        try { if (fs.existsSync(authStateDir)) fs.rmSync(authStateDir, { recursive: true, force: true }); } catch(e) {}
        try { if (fs.existsSync(SESSION_BACKUP_PATH)) fs.unlinkSync(SESSION_BACKUP_PATH); } catch(e) {}
        sock = null;
        connectionStatus = 'Disconnected';
        isConnecting = false;
        qrCodeBase64 = null;
        io.emit('status', { status: connectionStatus });
        io.emit('qr', { qr: null });
        setTimeout(() => connectToWhatsApp(), 1500);
        res.json({ success: true, message: 'Session force-cleared. Scan the new QR code.' });
    }
});

app.post('/api/send', async (req, res) => {
    const { number, text, image, voice } = req.body;

    if (!number) {
        return res.status(400).json({ success: false, message: 'Recipient number is required.' });
    }

    if (connectionStatus !== 'Connected' || !sock) {
        return res.status(503).json({ success: false, message: 'WhatsApp bot is not connected.' });
    }

    // Format phone number to WhatsApp JID format (e.g. "919895138430@s.whatsapp.net")
    let jid = number.trim();
    if (!jid.endsWith('@s.whatsapp.net') && !jid.endsWith('@g.us')) {
        // Remove non-digit characters
        jid = jid.replace(/\D/g, '');
        jid = `${jid}@s.whatsapp.net`;
    }

    try {
        addLog(`Manual message sending request to: ${jid}`);

        // 1. Send image if provided
        if (image) {
            const imgBuffer = Buffer.from(image.split(',')[1] || image, 'base64');
            const mimeMatch = image.match(/^data:([^;]+);base64,/);
            const mimetype = mimeMatch ? mimeMatch[1] : 'image/jpeg';
            
            // Write image to a temp file and send via url path to avoid stream buffer issues in Baileys
            const tempImgPath = path.join(__dirname, `temp_image_${Math.random().toString(36).substring(7)}.jpg`);
            fs.writeFileSync(tempImgPath, imgBuffer);
            try {
                await sock.sendMessage(jid, { 
                    image: { url: tempImgPath }, 
                    mimetype: mimetype,
                    caption: voice ? undefined : text 
                });
                addLog(`Image successfully sent to: ${jid}`);
            } finally {
                if (fs.existsSync(tempImgPath)) {
                    try { fs.unlinkSync(tempImgPath); } catch (e) {}
                }
            }
        }

        // 2. Send voice note if provided
        if (voice) {
            addLog('Transcoding manual browser audio to OGG/Opus...');
            const { filePath, isTranscoded } = await convertToOggOpusFile(voice);
            try {
                await sock.sendMessage(jid, { 
                    audio: { url: filePath }, 
                    mimetype: isTranscoded ? 'audio/ogg; codecs=opus' : 'audio/webm', 
                    ptt: true 
                });
                addLog(`Voice note successfully sent to: ${jid}`);
            } finally {
                if (fs.existsSync(filePath)) {
                    try { fs.unlinkSync(filePath); } catch (e) {}
                }
            }
            
            // If there's text (and optionally an image, since caption was ignored when voice note was sent), send text separately:
            if (text) {
                await sock.sendMessage(jid, { text });
                addLog(`Separated text message successfully sent to: ${jid}`);
            }
        }


        // 3. Send text message if only text is provided (no image, no voice)
        if (text && !image && !voice) {
            // Baileys v7 auto-generates link previews via generateWAMessageContent
            await sock.sendMessage(jid, { text });
            addLog(`Text message successfully sent to: ${jid}`);
        }

        res.json({ success: true, message: 'Message sent successfully.' });
    } catch (err) {
        addLog(`Failed to manually send message to ${jid}: ${err.message}`);
        res.status(500).json({ success: false, message: `Failed to send message: ${err.message}` });
    }
});

function parseInviteCode(link) {
    if (!link) return null;
    const match = link.match(/chat\.whatsapp\.com\/([a-zA-Z0-9]{22,24})/);
    return match ? match[1] : link.trim();
}

app.post('/api/groups/add-all', async (req, res) => {
    const { groupLink } = req.body;
    if (!groupLink) {
        return res.status(400).json({ success: false, message: 'Group link is required.' });
    }
    if (connectionStatus !== 'Connected' || !sock) {
        return res.status(503).json({ success: false, message: 'WhatsApp bot is not connected.' });
    }
    const inviteCode = parseInviteCode(groupLink);
    if (!inviteCode) {
        return res.status(400).json({ success: false, message: 'Invalid WhatsApp group link.' });
    }
    
    // Respond immediately to the frontend so it doesn't wait/timeout
    res.json({ success: true, message: 'Group addition process started in background.' });

    try {
        addLog(`[Group Add] Resolving group invite code: ${inviteCode}`);
        let groupJid = null;
        try {
            const inviteInfo = await sock.groupGetInviteInfo(inviteCode);
            groupJid = inviteInfo.id;
            addLog(`[Group Add] Group JID resolved: ${groupJid} (${inviteInfo.subject})`);
        } catch (e) {
            addLog(`[Group Add] Failed to get invite info, trying to accept invite/join...`);
            groupJid = await sock.groupAcceptInvite(inviteCode);
            addLog(`[Group Add] Group JID resolved after joining: ${groupJid}`);
        }

        if (!groupJid) {
            addLog(`[Group Add] Error: Could not resolve group JID for code ${inviteCode}`);
            return;
        }

        const contacts = loadActiveContacts();
        addLog(`[Group Add] Found ${contacts.length} active contacts to process.`);

        for (const jid of contacts) {
            try {
                addLog(`[Group Add] Adding participant: ${jid.split('@')[0]}`);
                const response = await sock.groupParticipantsUpdate(groupJid, [jid], "add");
                
                let resStatus = null;
                if (response && response[0]) {
                    resStatus = response[0].status;
                } else if (response && response[jid]) {
                    resStatus = response[jid].status;
                }

                addLog(`[Group Add] Response status for ${jid.split('@')[0]}: ${resStatus}`);

                if (resStatus === '403') {
                    addLog(`[Group Add] Private invite needed for ${jid.split('@')[0]}. Sending invite message...`);
                    await sock.sendMessage(jid, { 
                        text: `Hi! Join our official WhatsApp group here: ${groupLink}` 
                    });
                }
            } catch (err) {
                addLog(`[Group Add] Failed to add or invite ${jid.split('@')[0]}: ${err.message}`);
            }
            // Delay 3 seconds between requests to avoid WhatsApp spam filter triggering
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
        addLog(`[Group Add] Bulk addition process completed.`);
    } catch (err) {
        addLog(`[Group Add Error] Process failed: ${err.message}`);
    }
});

// ── /api/groups/add-all-chats: add every open chat thread (saved + unsaved) ─────
app.post('/api/groups/add-all-chats', async (req, res) => {
    const { groupLink } = req.body;
    if (!groupLink) {
        return res.status(400).json({ success: false, message: 'Group link is required.' });
    }
    if (connectionStatus !== 'Connected' || !sock) {
        return res.status(503).json({ success: false, message: 'WhatsApp bot is not connected.' });
    }
    const inviteCode = parseInviteCode(groupLink);
    if (!inviteCode) {
        return res.status(400).json({ success: false, message: 'Invalid WhatsApp group link.' });
    }

    // Flush chatMap into active_contacts.json before proceeding
    flushChatMap();

    // Respond immediately so the HTTP request doesn't time out
    res.json({ success: true, message: 'Add-all-chats process started in background.' });

    try {
        addLog(`[Group Add Chats] Resolving group invite code: ${inviteCode}`);
        let groupJid = null;
        try {
            const inviteInfo = await sock.groupGetInviteInfo(inviteCode);
            groupJid = inviteInfo.id;
            addLog(`[Group Add Chats] Group JID: ${groupJid} (${inviteInfo.subject})`);
        } catch (e) {
            groupJid = await sock.groupAcceptInvite(inviteCode);
            addLog(`[Group Add Chats] Group JID (after join): ${groupJid}`);
        }
        if (!groupJid) { addLog('[Group Add Chats] Could not resolve group JID.'); return; }

        const contacts = loadActiveContacts();
        // Only process individual chats (not groups — those end with @g.us)
        const individuals = contacts.filter(jid => jid.endsWith('@s.whatsapp.net'));
        addLog(`[Group Add Chats] ${individuals.length} individual chats to add (${contacts.length} total in DB, groups excluded).`);

        for (const jid of individuals) {
            try {
                addLog(`[Group Add Chats] Adding: ${jid.split('@')[0]}`);
                const response = await sock.groupParticipantsUpdate(groupJid, [jid], 'add');
                let resStatus = response?.[0]?.status ?? response?.[jid]?.status ?? null;
                addLog(`[Group Add Chats] Status for ${jid.split('@')[0]}: ${resStatus}`);
                if (resStatus === '403') {
                    await sock.sendMessage(jid, { text: `Hi! Join our group here: ${groupLink}` });
                    addLog(`[Group Add Chats] Sent invite link to ${jid.split('@')[0]}`);
                }
            } catch (err) {
                addLog(`[Group Add Chats] Direct add failed for ${jid.split('@')[0]}: ${err.message}. Sending invite message instead...`);
                try {
                    await sock.sendMessage(jid, { text: `Hi! Join our official WhatsApp group here: ${groupLink}` });
                    addLog(`[Group Add Chats] Sent invite link to ${jid.split('@')[0]}`);
                } catch (sendErr) {
                    addLog(`[Group Add Chats] Failed to send invite message to ${jid.split('@')[0]}: ${sendErr.message}`);
                }
            }
            await new Promise(r => setTimeout(r, 3000));
        }
        addLog('[Group Add Chats] Bulk addition completed.');
    } catch (err) {
        addLog(`[Group Add Chats Error] ${err.message}`);
    }
});

app.post('/api/groups/add-unsaved-chats', async (req, res) => {
    const { groupLink } = req.body;
    if (!groupLink) {
        return res.status(400).json({ success: false, message: 'Group link is required.' });
    }
    if (connectionStatus !== 'Connected' || !sock) {
        return res.status(503).json({ success: false, message: 'WhatsApp bot is not connected.' });
    }
    const inviteCode = parseInviteCode(groupLink);
    if (!inviteCode) {
        return res.status(400).json({ success: false, message: 'Invalid WhatsApp group link.' });
    }

    // Respond immediately to the frontend / client so it doesn't wait/timeout
    res.json({ success: true, message: 'Unsaved chats addition process started in background.' });

    try {
        addLog(`[Group Add Unsaved] Resolving group invite code: ${inviteCode}`);
        let groupJid = null;
        try {
            const inviteInfo = await sock.groupGetInviteInfo(inviteCode);
            groupJid = inviteInfo.id;
            addLog(`[Group Add Unsaved] Group JID resolved: ${groupJid} (${inviteInfo.subject})`);
        } catch (e) {
            addLog(`[Group Add Unsaved] Failed to get invite info, trying to accept invite/join...`);
            groupJid = await sock.groupAcceptInvite(inviteCode);
            addLog(`[Group Add Unsaved] Group JID resolved after joining: ${groupJid}`);
        }

        if (!groupJid) {
            addLog(`[Group Add Unsaved] Error: Could not resolve group JID for code ${inviteCode}`);
            return;
        }

        const active = loadActiveContacts();
        let saved = [];
        const savedContactsFile = path.join(__dirname, 'saved_contacts.json');
        if (fs.existsSync(savedContactsFile)) {
            try {
                saved = JSON.parse(fs.readFileSync(savedContactsFile, 'utf8'));
            } catch (e) {
                addLog(`[Group Add Unsaved] Failed to load saved_contacts.json: ${e.message}`);
            }
        }
        
        // Filter out contacts, only keep raw chats that are NOT saved contacts
        const unsaved = active.filter(jid => !saved.includes(jid));
        addLog(`[Group Add Unsaved] Found ${active.length} active chats, ${saved.length} saved contacts. Unsaved chats to process: ${unsaved.length}`);

        for (const jid of unsaved) {
            try {
                addLog(`[Group Add Unsaved] Adding participant: ${jid.split('@')[0]}`);
                const response = await sock.groupParticipantsUpdate(groupJid, [jid], "add");
                
                let resStatus = null;
                if (response && response[0]) {
                    resStatus = response[0].status;
                } else if (response && response[jid]) {
                    resStatus = response[jid].status;
                }

                addLog(`[Group Add Unsaved] Response status for ${jid.split('@')[0]}: ${resStatus}`);

                if (resStatus === '403') {
                    addLog(`[Group Add Unsaved] Private invite needed for ${jid.split('@')[0]}. Sending invite message...`);
                    await sock.sendMessage(jid, { 
                        text: `Hi! Join our official WhatsApp group here: ${groupLink}` 
                    });
                }
            } catch (err) {
                addLog(`[Group Add Unsaved] Failed to add or invite ${jid.split('@')[0]}: ${err.message}`);
            }
            // Delay 3 seconds between requests to avoid WhatsApp spam filter triggering
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
        addLog(`[Group Add Unsaved] Bulk unsaved addition process completed.`);
    } catch (err) {
        addLog(`[Group Add Unsaved Error] Process failed: ${err.message}`);
    }
});

app.post('/api/export-session', (req, res) => {
    const authDir = AUTH_DIR;
    try {
        if (!fs.existsSync(authDir)) {
            return res.status(404).json({ success: false, message: 'No session to export.' });
        }
        const files = {};
        fs.readdirSync(authDir).forEach(f => {
            files[f] = fs.readFileSync(path.join(authDir, f), 'base64');
        });
        res.json({ success: true, session: Buffer.from(JSON.stringify(files)).toString('base64') });
    } catch(e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

app.post('/api/import-session', async (req, res) => {
    const { session } = req.body;
    if (!session) return res.status(400).json({ success: false, message: 'No session data provided.' });
    const authDir = AUTH_DIR;
    try {
        const files = JSON.parse(Buffer.from(session, 'base64').toString('utf8'));
        if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });
        Object.entries(files).forEach(([name, b64]) => {
            fs.writeFileSync(path.join(authDir, name), Buffer.from(b64, 'base64'));
        });
        // Also update the /tmp backup
        fs.writeFileSync(SESSION_BACKUP_PATH, JSON.stringify(files));
        addLog('Session imported. Reconnecting...');
        if (sock) { try { sock.end(); } catch(e) {} sock = null; }
        connectionStatus = 'Disconnected';
        isConnecting = false;
        setTimeout(() => connectToWhatsApp(), 1000);
        res.json({ success: true, message: 'Session imported. Reconnecting now.' });
    } catch(e) {
        res.status(500).json({ success: false, message: 'Invalid session data: ' + e.message });
    }
});

// Socket connection listener
io.on('connection', (socket) => {
    socket.emit('status', { status: connectionStatus });
    socket.emit('qr', { qr: qrCodeBase64 });
    socket.emit('keywords', loadKeywords());
    socket.emit('logs-init', logs);
});

let sock = null;
let qrCodeBase64 = null;
let connectionStatus = 'Disconnected'; // Disconnected, Connecting, Connected, Scanning
let isConnecting = false;

// Plain in-memory chat map — populated from Baileys events on every connection
const chatMap = new Map(); // jid -> true
function registerChat(id) {
    if (id && typeof id === 'string' && !chatMap.has(id)) {
        chatMap.set(id, true);
        addContact(id); // also persist to active_contacts.json
    }
}
function flushChatMap() {
    chatMap.forEach((_, id) => addContact(id));
    addLog(`[Chat Sync] Flushed ${chatMap.size} chats from in-memory map.`);
}

async function connectToWhatsApp() {
    if (isConnecting) return;
    isConnecting = true;

    // Use AUTH_DIR env var for Railway persistent volume, fallback to local
    const authStateDir = AUTH_DIR;
    
    // Attempt to restore session from /tmp backup if auth dir is empty or missing
    if (!fs.existsSync(authStateDir) || fs.readdirSync(authStateDir).length === 0) {
        restoreSession(authStateDir);
    }
    
    const { state, saveCreds } = await useMultiFileAuthState(authStateDir);
    
    // Fetch latest WhatsApp Web version to prevent protocol mismatch errors
    let version = [2, 3000, 1034074495]; // Safe fallback — updated for 2025 protocol
    try {
        const { version: latestVer, isLatest } = await fetchLatestBaileysVersion();
        version = latestVer;
        addLog(`Fetched latest WhatsApp Web version: ${version.join('.')}, isLatest: ${isLatest}`);
    } catch (err) {
        addLog(`Could not fetch latest WA version, using fallback: ${err.message}`);
    }

    addLog('Initializing WhatsApp socket connection (with active persistent volume)...');
    connectionStatus = 'Connecting';
    io.emit('status', { status: connectionStatus });

    try {
        sock = makeWASocket({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'silent' }),
            // Browser fingerprint — required by Baileys v7 to avoid 401 handshake failures
            browser: ['WhatsApp Bot', 'Chrome', '125.0.0'],
            // Prevent premature timeout during QR auth (important for v7)
            defaultQueryTimeoutMs: undefined,
            // Keep connection alive
            keepAliveIntervalMs: 25000,
            // Generate high quality link preview
            generateHighQualityLinkPreview: true,
        });

        sock.ev.on('creds.update', async () => {
            await saveCreds();
            // Also back up to /tmp so session survives Railway container restarts
            backupSession(authStateDir);
        });

        sock.ev.on('messaging-history.set', ({ chats, contacts, messages }) => {
            if (chats) {
                chats.forEach(c => { registerChat(c.id); addContact(c.id); });
                addLog(`[History Sync] Loaded ${chats.length} chats from history.`);
            }
            if (contacts) {
                try {
                    let savedJids = [];
                    const savedContactsFile = path.join(__dirname, 'saved_contacts.json');
                    if (fs.existsSync(savedContactsFile)) {
                        savedJids = JSON.parse(fs.readFileSync(savedContactsFile, 'utf8'));
                    }
                    let updated = false;
                    contacts.forEach(c => {
                        if (c.id && c.id.endsWith('@s.whatsapp.net') && !savedJids.includes(c.id)) {
                            savedJids.push(c.id);
                            updated = true;
                        }
                    });
                    if (updated) {
                        fs.writeFileSync(savedContactsFile, JSON.stringify(savedJids, null, 2), 'utf8');
                    }
                    addLog(`[History Sync] Loaded ${contacts.length} contacts from history.`);
                } catch(e) {
                    console.error('Error handling contacts in history set:', e);
                }
            }
        });

        // Sync chats list to maintain our active inbox contacts database
        sock.ev.on('chats.set', ({ chats }) => {
            if (chats) {
                chats.forEach(c => { registerChat(c.id); addContact(c.id); });
                addLog(`[Chat Sync] chats.set: ${chats.length} chats loaded.`);
            }
        });

        sock.ev.on('chats.upsert', (chats) => {
            if (chats) {
                chats.forEach(c => { registerChat(c.id); addContact(c.id); });
            }
        });

        sock.ev.on('contacts.set', ({ contacts }) => {
            if (contacts) {
                try {
                    const savedJids = contacts.map(c => c.id).filter(id => id && id.endsWith('@s.whatsapp.net'));
                    const savedContactsFile = path.join(__dirname, 'saved_contacts.json');
                    fs.writeFileSync(savedContactsFile, JSON.stringify(savedJids, null, 2), 'utf8');
                    addLog(`[Contact Sync] Synced ${savedJids.length} saved contacts.`);
                } catch(e) {
                    console.error('Error saving contacts:', e);
                }
            }
        });

        sock.ev.on('contacts.upsert', (contacts) => {
            if (contacts) {
                try {
                    let savedJids = [];
                    const savedContactsFile = path.join(__dirname, 'saved_contacts.json');
                    if (fs.existsSync(savedContactsFile)) {
                        savedJids = JSON.parse(fs.readFileSync(savedContactsFile, 'utf8'));
                    }
                    let updated = false;
                    contacts.forEach(c => {
                        if (c.id && c.id.endsWith('@s.whatsapp.net') && !savedJids.includes(c.id)) {
                            savedJids.push(c.id);
                            updated = true;
                        }
                    });
                    if (updated) {
                        fs.writeFileSync(savedContactsFile, JSON.stringify(savedJids, null, 2), 'utf8');
                    }
                } catch(e) {
                    console.error('Error updating contacts:', e);
                }
            }
        });

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            
            if (qr) {
                connectionStatus = 'Scanning';
                io.emit('status', { status: connectionStatus });
                try {
                    qrCodeBase64 = await QRCode.toDataURL(qr);
                    io.emit('qr', { qr: qrCodeBase64 });
                    addLog('New QR Code generated. Scan it via the web dashboard.');
                } catch (err) {
                    addLog('Error converting QR code to base64 image.');
                }
            }

            if (connection === 'close') {
                isConnecting = false;
                const statusCode = lastDisconnect?.error?.output?.statusCode || lastDisconnect?.error?.code;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                
                connectionStatus = 'Disconnected';
                qrCodeBase64 = null;
                io.emit('status', { status: connectionStatus });
                io.emit('qr', { qr: null });
                
                addLog(`Connection closed. Error: ${lastDisconnect?.error?.message || 'Unknown reason'} (Status code: ${statusCode}). Reconnecting: ${shouldReconnect}`);
                
                if (shouldReconnect) {
                    addLog('Reconnecting in 5 seconds...');
                    setTimeout(() => connectToWhatsApp(), 5000);
                } else {
                    addLog('Logged out from WhatsApp. Clearing session credentials...');
                    try {
                        fs.rmSync(authStateDir, { recursive: true, force: true });
                        addLog('Session directory cleared successfully.');
                    } catch (e) {
                        addLog(`Failed to clear session directory: ${e.message}`);
                    }
                    try {
                        if (fs.existsSync(SESSION_BACKUP_PATH)) {
                            fs.unlinkSync(SESSION_BACKUP_PATH);
                            addLog('Session backup file cleared successfully.');
                        }
                    } catch (e) {
                        addLog(`Failed to clear session backup file: ${e.message}`);
                    }
                    addLog('Restarting connection for fresh QR code in 3 seconds...');
                    setTimeout(() => connectToWhatsApp(), 3000);
                }
            } else if (connection === 'open') {
                isConnecting = false;
                connectionStatus = 'Connected';
                qrCodeBase64 = null;
                // Backup fresh session immediately after successful QR scan
                backupSession(authStateDir);
                addLog('WhatsApp Connection established successfully!');

                io.emit('status', { status: connectionStatus });
                io.emit('qr', { qr: null });

                // Flush in-memory chat map into active_contacts.json on every connect
                setTimeout(() => {
                    try {
                        flushChatMap();
                    } catch(e) {
                        addLog(`[Chat Sync] Store flush failed: ${e.message}`);
                    }
                }, 5000); // 5s delay to let WhatsApp finish sending initial sync payloads
            } else if (connection === 'connecting') {
                connectionStatus = 'Connecting';
                io.emit('status', { status: connectionStatus });
                addLog('Connecting to WhatsApp API...');
            }
        });

        sock.ev.on('messages.upsert', async (m) => {
            if (m.type !== 'notify') return;
            
            for (const msg of m.messages) {
                // Track contacts from every message exchange
                if (msg.key && msg.key.remoteJid) {
                    addContact(msg.key.remoteJid);
                }
                const partJid = msg.key?.participant || msg.participant;
                if (partJid) {
                    addContact(partJid);
                }

                if (msg.key.fromMe) continue;
                
                const messageType = Object.keys(msg.message || {})[0];
                let text = '';
                if (messageType === 'conversation') {
                    text = msg.message.conversation;
                } else if (messageType === 'extendedTextMessage') {
                    text = msg.message.extendedTextMessage.text;
                }
                
                if (!text) continue;
                
                const senderJid = msg.key.remoteJid;
                const senderName = msg.pushName || 'WhatsApp User';
                addLog(`Received message from ${senderName} (${senderJid}): "${text}"`);
                // Store the original message for quoted reply
                const quotedMsg = msg;
                
                // Match keywords
                const kwMap = loadKeywords();
                const cleanText = text.trim().toLowerCase();
                
                for (const [kwPattern, ruleData] of Object.entries(kwMap)) {
                    // Support comma-separated keywords (e.g. "host, hoster, hostinger")
                    const keywords = kwPattern.split(',').map(k => k.trim().toLowerCase()).filter(k => k.length > 0);
                    const isMatch = keywords.some(kw => matchKeyword(cleanText, kw));
                    
                    if (isMatch) {
                        addLog(`Keyword match found in pattern "${kwPattern}". Replying automatically...`);
                        try {
                            // Normalize simple string rules to object rules for backward compatibility
                            const rule = typeof ruleData === 'string'
                                ? { text: ruleData, image: null, voice: null }
                                : ruleData;
                                
                            const { text: replyText, image: replyImage, voice: replyVoice } = rule;

                            const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

                            // Mark message as read BEFORE replying - critical for delivery
                            try {
                                await sock.readMessages([msg.key]);
                            } catch (e) {}

                            // Subscribe to sender's presence so WhatsApp doesn't treat us as offline bot
                            try {
                                await sock.presenceSubscribe(senderJid);
                            } catch (e) {}
                            const setPresence = async (type) => {
                                try {
                                    await sock.sendPresenceUpdate(type, senderJid);
                                } catch (e) {}
                            };

                            // Initial reaction delay (simulate human reading/noticing)
                            await delay(1000);

                            // 1. Send image if provided
                            if (replyImage) {
                                await setPresence('composing');
                                await delay(2000); // Simulate upload/processing time
                                
                                const imgBuffer = Buffer.from(replyImage.split(',')[1] || replyImage, 'base64');
                                const mimeMatch = replyImage.match(/^data:([^;]+);base64,/);
                                const mimetype = mimeMatch ? mimeMatch[1] : 'image/jpeg';
                                addLog(`Image buffer size: ${imgBuffer.length} bytes, mimetype: ${mimetype}`);
                                
                                const tempImgPath = path.join(__dirname, `temp_image_${Math.random().toString(36).substring(7)}.jpg`);
                                fs.writeFileSync(tempImgPath, imgBuffer);
                                try {
                                    await sock.sendMessage(senderJid, { 
                                        image: { url: tempImgPath }, 
                                        mimetype: mimetype,
                                        caption: replyVoice ? undefined : replyText 
                                    });
                                    addLog(`Auto-reply sent image to ${senderName}.`);
                                } finally {
                                    if (fs.existsSync(tempImgPath)) {
                                        try { fs.unlinkSync(tempImgPath); } catch (e) {}
                                    }
                                }
                                
                                await setPresence('paused');
                                await delay(1500); // Small interval between messages
                            }

                            // 2. Send voice note if provided
                            if (replyVoice) {
                                await setPresence('recording');
                                addLog('Transcoding auto-reply audio to OGG/Opus...');
                                const { filePath, isTranscoded } = await convertToOggOpusFile(replyVoice);
                                
                                try {
                                    await delay(3500); // Simulate audio recording duration
                                    await sock.sendMessage(senderJid, { 
                                        audio: { url: filePath }, 
                                        mimetype: isTranscoded ? 'audio/ogg; codecs=opus' : 'audio/webm', 
                                        ptt: true 
                                    });
                                    addLog(`Auto-reply sent voice note to ${senderName}.`);
                                } finally {
                                    if (fs.existsSync(filePath)) {
                                        try { fs.unlinkSync(filePath); } catch (e) {}
                                    }
                                }
                                
                                await setPresence('paused');
                                await delay(1500); // Small interval before next message
                                
                                if (replyText) {
                                    await setPresence('composing');
                                    const typingDuration = Math.min(1500 + replyText.length * 15, 6000);
                                    await delay(typingDuration);
                                    
                                    await sock.sendMessage(senderJid, { text: replyText });
                                    addLog(`Auto-reply sent text message separately to ${senderName}.`);
                                    
                                    await setPresence('paused');
                                }
                            }

                            // 3. Send text message if only text is provided (no image, no voice)
                            if (replyText && !replyImage && !replyVoice) {
                                await setPresence('composing');
                                const typingDuration = Math.min(1500 + replyText.length * 15, 6000);
                                await delay(typingDuration);

                                // Baileys v7 auto-generates link previews via generateWAMessageContent
                                // when sendMessage is called with { text } — no manual linkPreview needed
                                await sock.sendMessage(senderJid, { text: replyText });
                                addLog(`Auto-reply sent text to ${senderName}.`);
                                
                                await setPresence('paused');
                            }

                            break; // Stop after first match
                        } catch (err) {
                            addLog(`Failed to send message: ${err.message}`);
                        }
                    }
                }
            }
        });

        // Track message delivery acknowledgements (1=sent, 2=delivered, 3=read, -1=error)
        sock.ev.on('messages.update', (updates) => {
            for (const update of updates) {
                if (update.key.fromMe) {
                    const ack = update.update?.status;
                    if (ack === 1) addLog(`📤 Message sent (1 tick) to ${update.key.remoteJid?.split('@')[0]}`);
                    else if (ack === 2) addLog(`✅ Message delivered (2 ticks) to ${update.key.remoteJid?.split('@')[0]}`);
                    else if (ack === 3) addLog(`👁️ Message read by ${update.key.remoteJid?.split('@')[0]}`);
                    else if (ack === -1) addLog(`❌ Message failed/rejected by WhatsApp for ${update.key.remoteJid?.split('@')[0]}`);
                }
            }
        });
    } catch (err) {
        isConnecting = false;
        addLog(`Error initializing socket: ${err.message}`);
        setTimeout(() => connectToWhatsApp(), 5000);
    }
}

server.listen(PORT, () => {
    addLog(`Server is running on port ${PORT}`);
    addLog(`Dashboard URL: http://localhost:${PORT}`);
    addLog(`Persistent storage path: ${persistentDir || 'None (using local fallback)'}`);
    connectToWhatsApp();
    startYTBot();
    startFBBot();
});

