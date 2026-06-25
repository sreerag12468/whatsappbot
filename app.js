import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion, getUrlInfo, DisconnectReason } from '@whiskeysockets/baileys';
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

const execPromise = promisify(exec);

// Resolve __dirname in ES module context
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Helper function to transcode audio to strict OGG/Opus format using FFmpeg
async function convertToOggOpus(base64Data) {
    const tempDir = path.join(__dirname, 'temp_audio');
    if (!fs.existsSync(tempDir)) {
        fs.mkdirSync(tempDir);
    }
    
    const randomId = Math.random().toString(36).substring(7);
    const inputPath = path.join(tempDir, `input_${randomId}.webm`);
    const outputPath = path.join(tempDir, `output_${randomId}.ogg`);
    
    try {
        const buffer = Buffer.from(base64Data.split(',')[1] || base64Data, 'base64');
        fs.writeFileSync(inputPath, buffer);
        
        // Transcode WebM/MP4 audio to Mono, 48kHz, Opus codec OGG container for WhatsApp compatibility
        const command = `ffmpeg -y -i "${inputPath}" -vn -c:a libopus -b:a 48k -ac 1 -avoid_negative_ts make_zero -f ogg "${outputPath}"`;
        await execPromise(command);
        
        const oggBuffer = fs.readFileSync(outputPath);
        return oggBuffer;
    } catch (err) {
        console.error(`[FFmpeg Error] ${err.message}`);
        // Fallback to sending raw buffer if transcode fails
        return Buffer.from(base64Data.split(',')[1] || base64Data, 'base64');
    } finally {
        try {
            if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
            if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
        } catch (e) {}
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
const KEYWORDS_PATH = path.join(__dirname, 'keywords.json');
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

app.get('/api/logs', (req, res) => {
    res.json(logs);
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
            // If text is provided and there is no voice note, send the text as caption
            await sock.sendMessage(jid, { 
                image: imgBuffer, 
                caption: voice ? undefined : text 
            });
            addLog(`Image successfully sent to: ${jid}`);
        }

        // 2. Send voice note if provided
        if (voice) {
            addLog('Transcoding manual browser audio to OGG/Opus...');
            const voiceBuffer = await convertToOggOpus(voice);
            await sock.sendMessage(jid, { 
                audio: voiceBuffer, 
                mimetype: 'audio/ogg; codecs=opus', 
                ptt: true 
            });
            addLog(`Voice note successfully sent to: ${jid}`);
            
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
                const url = urls[0];
                try {
                    linkPreview = await getUrlInfo(url);
                } catch (urlErr) {
                    addLog(`Failed to generate link preview: ${urlErr.message}`);
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

    const authStateDir = path.join(__dirname, 'auth_info_baileys');
    const { state, saveCreds } = await useMultiFileAuthState(authStateDir);
    
    // Fetch latest WhatsApp Web version to prevent protocol mismatch errors (e.g. error code 405/411)
    let version = [2, 3000, 1017578426]; // Safe fallback version
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
        sock = makeWASocket.default ? makeWASocket.default({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'silent' }),
        }) : makeWASocket({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'silent' }),
        });

        sock.ev.on('creds.update', saveCreds);

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
                    addLog('Restarting connection for fresh QR code in 3 seconds...');
                    setTimeout(() => connectToWhatsApp(), 3000);
                }
            } else if (connection === 'open') {
                isConnecting = false;
                connectionStatus = 'Connected';
                qrCodeBase64 = null;
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
                addLog(`Received message from ${senderName} (${senderJid.split('@')[0]}): "${text}"`);
                
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
                                await sock.sendMessage(senderJid, { 
                                    image: imgBuffer, 
                                    caption: replyVoice ? undefined : replyText 
                                });
                                addLog(`Auto-reply sent image to ${senderName}.`);
                                
                                await setPresence('paused');
                                await delay(1500); // Small interval between messages
                            }

                            // 2. Send voice note if provided
                            if (replyVoice) {
                                await setPresence('recording');
                                addLog('Transcoding auto-reply audio to OGG/Opus...');
                                const voiceBuffer = await convertToOggOpus(replyVoice);
                                
                                await delay(3500); // Simulate audio recording duration
                                await sock.sendMessage(senderJid, { 
                                    audio: voiceBuffer, 
                                    mimetype: 'audio/ogg; codecs=opus', 
                                    ptt: true 
                                });
                                addLog(`Auto-reply sent voice note to ${senderName}.`);
                                
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
                                    const url = urls[0];
                                    try {
                                        linkPreview = await getUrlInfo(url);
                                    } catch (urlErr) {
                                        addLog(`Failed to generate link preview: ${urlErr.message}`);
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

