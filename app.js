import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason, getUrlInfo } from '@whiskeysockets/baileys';
import { getLinkPreview } from 'link-preview-js';
import pino from 'pino';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import QRCode from 'qrcode';
import fs from 'fs';
import path from 'path';
import cors from 'cors';
import { fileURLToPath } from 'url';
import { exec } from 'child_process';
import { promisify } from 'util';
import ffmpegPath from 'ffmpeg-static';

const execPromise = promisify(exec);

// Session backup/restore helpers
// Saves auth_info_baileys as base64 JSON to a backup file so sessions survive Railway restarts
const SESSION_BACKUP_PATH = '/tmp/wa_session_backup.json';

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
const KEYWORDS_PATH = process.env.KEYWORDS_PATH || path.join(__dirname, 'keywords.json');
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

// Ensure keywords file exists with a default empty object if it doesn't
if (!fs.existsSync(KEYWORDS_PATH)) {
    try {
        fs.writeFileSync(KEYWORDS_PATH, '{}', 'utf8');
    } catch (e) {
        console.error('Failed to create default keywords.json:', e);
    }
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

// Server setup
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
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
        const authStateDir = process.env.AUTH_DIR || path.join(__dirname, 'auth_info_baileys');
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
        const authStateDir = process.env.AUTH_DIR || path.join(__dirname, 'auth_info_baileys');
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
            // Check for links and get preview if any
            const urlRegex = /https?:\/\/[^\s]+/gi;
            const urls = text.match(urlRegex);
            let linkPreview = undefined;
            
            if (urls && urls.length > 0) {
                // Give time for the link to load as requested by the user
                addLog(`Link detected in manual message. Delaying for 3 seconds to let the link load...`);
                await new Promise(resolve => setTimeout(resolve, 3000));
                
                try {
                    linkPreview = await getUrlInfo(text, {
                        uploadImage: sock.waUploadToServer
                    });
                } catch (urlErr) {
                    addLog(`Failed to generate link preview using getUrlInfo: ${urlErr.message}`);
                    // Fallback to link-preview-js if getUrlInfo fails
                    try {
                        const previewData = await getLinkPreview(urls[0]);
                        linkPreview = {
                            'canonical-url': previewData.url || urls[0],
                            'matched-text': urls[0],
                            title: previewData.title || '',
                            description: previewData.description || '',
                        };
                    } catch (fallbackErr) {}
                }
            }
            
            const msgContent = { text };
            if (linkPreview) {
                msgContent.linkPreview = linkPreview;
            }
            await sock.sendMessage(jid, msgContent);
            addLog(`Text message successfully sent to: ${jid}`);
        }

        res.json({ success: true, message: 'Message sent successfully.' });
    } catch (err) {
        addLog(`Failed to manually send message to ${jid}: ${err.message}`);
        res.status(500).json({ success: false, message: `Failed to send message: ${err.message}` });
    }
});

app.post('/api/export-session', (req, res) => {
    const authDir = process.env.AUTH_DIR || path.join(__dirname, 'auth_info_baileys');
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
    const authDir = process.env.AUTH_DIR || path.join(__dirname, 'auth_info_baileys');
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

async function connectToWhatsApp() {
    if (isConnecting) return;
    isConnecting = true;

    // Use AUTH_DIR env var for Railway persistent volume, fallback to local
    const authStateDir = process.env.AUTH_DIR || path.join(__dirname, 'auth_info_baileys');
    
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

    addLog('Initializing WhatsApp socket connection...');
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
                addLog('WhatsApp Connection established successfully!');
            } else if (connection === 'connecting') {
                connectionStatus = 'Connecting';
                io.emit('status', { status: connectionStatus });
                addLog('Connecting to WhatsApp API...');
            }
        });

        sock.ev.on('messages.upsert', async (m) => {
            if (m.type !== 'notify') return;
            
            for (const msg of m.messages) {
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

                                const urlRegex = /https?:\/\/[^\s]+/gi;
                                const urls = replyText.match(urlRegex);
                                let linkPreview = undefined;
                                
                                if (urls && urls.length > 0) {
                                    // Give time for the link to load as requested by the user
                                    addLog(`Link detected in auto-reply. Delaying for 3 seconds to let the link load...`);
                                    await delay(3000);
                                    
                                    try {
                                        linkPreview = await getUrlInfo(replyText, {
                                            uploadImage: sock.waUploadToServer
                                        });
                                    } catch (urlErr) {
                                        addLog(`Failed to generate auto-reply link preview: ${urlErr.message}`);
                                        // Fallback
                                        try {
                                            const previewData = await getLinkPreview(urls[0]);
                                            linkPreview = {
                                                'canonical-url': previewData.url || urls[0],
                                                'matched-text': urls[0],
                                                title: previewData.title || '',
                                                description: previewData.description || '',
                                            };
                                        } catch (fallbackErr) {}
                                    }
                                }

                                
                                const msgContent = { text: replyText };
                                if (linkPreview) {
                                    msgContent.linkPreview = linkPreview;
                                }
                                await sock.sendMessage(senderJid, msgContent);
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    addLog(`Server is running on port ${PORT}`);
    addLog(`Dashboard URL: http://localhost:${PORT}`);
    connectToWhatsApp();
});

