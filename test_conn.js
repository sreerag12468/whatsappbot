import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason } from '@whiskeysockets/baileys';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function test() {
    console.log("Fetching latest WA version...");
    try {
        const { version, isLatest } = await fetchLatestBaileysVersion();
        console.log(`Using WA v${version.join('.')}, isLatest: ${isLatest}`);

        const authStateDir = path.join(__dirname, 'auth_info_baileys_test');
        const { state, saveCreds } = await useMultiFileAuthState(authStateDir);

        console.log("Creating socket...");
        const sock = makeWASocket.default ? makeWASocket.default({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'debug' }),
        }) : makeWASocket({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: 'debug' }),
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', (update) => {
            const { connection, lastDisconnect, qr } = update;
            console.log("Connection update event:", connection, lastDisconnect ? {
                message: lastDisconnect.error?.message,
                code: lastDisconnect.error?.code,
                statusCode: lastDisconnect.error?.output?.statusCode,
                stack: lastDisconnect.error?.stack,
                error: lastDisconnect.error
            } : null, qr ? "QR received" : null);
        });
    } catch (e) {
        console.error("Test failed with error:", e);
    }
}

test();
